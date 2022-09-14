defmodule Membrane.MPEG.TS.Demuxer do
  @moduledoc """
  Demuxes MPEG TS stream.

  After transition into playing state, this element will wait for [Program
  Association Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PAT)
  and [Program Mapping
  Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PMT). Upon
  succesfful parsing of those tables it will send a message to the pipeline in
  format `{:mpeg_ts_stream_info, configuration}`, where configuration contains
  data read from tables.

  Configuration sent by element to pipeline has following shape
  ```
  %MPEG.TS.PMT{
    pcr_pid: 256,
    program_info: [],
    streams: %{
      256 => %{stream_type: :H264, stream_type_id: 27},
      257 => %{stream_type: :MPEG1_AUDIO, stream_type_id: 3}
    }
  }

  ```
  """
  use Membrane.Filter

  alias MPEG.TS

  def_input_pad(:input, caps: :any, demand_unit: :buffers)
  def_output_pad(:output, availability: :on_request, caps: :any)

  @type state_t :: :waiting_pmt | :online

  @initial_state %{
    state: :waiting_pmt,
    demuxer: TS.Demuxer.new(),
    pad_lut: %{},
    bytes_buffer: <<>>,
    pending_demand: %{}
  }

  @impl true
  def handle_init(_) do
    {:ok, @initial_state}
  end

  @impl true
  def handle_pad_added(pad = {Membrane.Pad, _, {:stream_id, _sid}}, _, state) do
    # TODO: as soon as we've parse the PMT at this point, we can actually tell
    # something abount the data format (see PMT stream_type). Replace :any with
    # that information ASAP.
    {{:ok, [{:caps, {pad, :any}}]}, state}
  end

  @impl true
  def handle_prepared_to_stopped(_ctx, _state), do: {:ok, @initial_state}

  @impl true
  def handle_prepared_to_playing(_ctx, state) do
    # Output pad is connected after a PMT table is recognized, hence we have to
    # ask for the first packets by ourselves.
    {{:ok, demand: Pad.ref(:input)}, state}
  end

  @impl true
  def handle_demand(pad, size, :buffers, _ctx, state) do
    pending = Map.update(state.pending_demand, pad, 0, fn old -> old + size end)
    state = %{state | pending_demand: pending}

    {fulfill_actions, state} = fulfill_pending_demand(state)
    {refresh_actions, state} = forward_demand(state)

    {{:ok, fulfill_actions ++ refresh_actions}, state}
  end

  @impl true
  def handle_process(:input, buffer, _ctx, state) do
    {packets, to_buffer} = parse_buffer(state.bytes_buffer <> buffer)
    demuxer = TS.Demuxer.push(state.demuxer, packets)
    state = %{state | bytes_buffer: to_buffer, demuxer: demuxer}

    {notifications, state} =
      if state.state == :waiting_pmt && TS.Demuxer.has_pmt?(demuxer) do
        actions = [
          {:notify, {:mpeg_ts_stream_info, TS.Demuxer.get_pmt!(demuxer)}}
        ]

        state = %{state | state: :online}
        {actions, state}
      else
        {[], state}
      end

    {fulfill_actions, state} = fulfill_pending_demand(state)
    {{:ok, notifications ++ fulfill_actions}, state}
  end

  defp parse_buffer(buffer) do
    {ok, err} =
      buffer
      |> TS.Packet.parse_many()
      |> Enum.split_with(fn tuple -> elem(tuple, 0) == :ok end)

    # fail fast in case a critical error is encountered. If data becomes
    # mis-aligned this module should be responsible for fixing it.
    critical_err =
      Enum.find(err, fn tuple ->
        elem(tuple, 0) == :error && elem(tuple, 1) != :not_enough_data
      end)

    if critical_err != nil do
      raise ArgumentError, "MPEG-TS unrecoverable parse error: #{inspect(elem(critical_err, 1))}"
    end

    to_buffer =
      err
      |> Enum.filter(fn
        {:error, :not_enough_data, _} -> true
        _ -> false
      end)
      |> Enum.map(fn {_, _, data} -> data end)
      |> Enum.reduce(<<>>, fn x, acc -> acc <> x end)

    ok = Enum.map(ok, fn {:ok, x} -> x end)

    {ok, to_buffer}
  end

  defp fulfill_pending_demand(state) do
    {pad_with_packets, demuxer} =
      state.pending_demand
      |> Enum.map_reduce(state.demuxer, fn {pad, size}, demuxer ->
        {Membrane.Pad, _, {:stream_id, sid}} = pad
        {demuxer, packets} = TS.Demuxer.packets_from_stream(demuxer, sid, size)
        missing = size - length(packets)
        {{pad, packets, missing}, demuxer}
      end)

    actions =
      pad_with_packets
      |> Enum.flat_map(fn {pad, packets, _missing} ->
        packets
        |> Enum.map(fn {unm = MPEG.TS.PartialPES, packet} ->
          case unm.unmarshal(packet) do
            {:ok, pes} ->
              pes

            {:error, reason} ->
              raise ArgumentError,
                    "MPEG-TS could not parse Partial PES packet: #{inspect(reason)}"
          end
        end)
        |> Enum.map(fn pes -> {:buffer, {pad, %Membrane.Buffer{payload: pes.data}}} end)
      end)

    pending_demand =
      pad_with_packets
      |> Enum.map(fn {pad, _, missing} -> {pad, missing} end)
      |> Enum.into(%{})

    {actions, %{state | pending_demand: pending_demand, demuxer: demuxer}}
  end

  defp forward_demand(state) do
    actions =
      state.pending_demand
      |> Enum.map(fn {pad, size} -> {:demand, {pad, size}} end)

    {actions, state}
  end
end
