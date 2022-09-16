defmodule Membrane.MPEG.TS.Demuxer do
  @moduledoc """
  Demuxes MPEG TS stream.

  After transition into playing state, this element will wait for [Program
  Association Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PAT)
  and [Program Mapping
  Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PMT).
  ```
  """
  use Membrane.Filter

  alias MPEG.TS

  def_input_pad(:input, caps: :any, demand_unit: :buffers)
  def_output_pad(:output, availability: :on_request, caps: :any)

  @type state_t :: :waiting_pmt | :online

  @initial_state %{
    state: :waiting_pmt,
    demuxer: TS.Demuxer.new([]),
    pending_demand: %{},
    closed: false
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
  def handle_prepared_to_stopped(_ctx, _state) do
    {:ok, @initial_state}
  end

  @impl true
  def handle_prepared_to_playing(_ctx, state) do
    # Output pad is connected after a PMT table is recognized, hence we have to
    # ask for the first packets by ourselves.
    {{:ok, demand: Pad.ref(:input)}, state}
  end

  @impl true
  def handle_demand(pad, size, :buffers, _ctx, state) do
    pending = Map.update(state.pending_demand, pad, size, fn old -> old + size end)
    state = %{state | pending_demand: pending}

    {fulfill_actions, state} = fulfill_pending_demand(state)
    {refresh_actions, state} = forward_demand(state)

    {{:ok, fulfill_actions ++ refresh_actions}, state}
  end

  @impl true
  def handle_process(:input, buffer, _ctx, state) do
    demuxer = TS.Demuxer.push_buffer(state.demuxer, buffer.payload)
    state = %{state | demuxer: demuxer}

    {extra_actions, state} =
      cond do
        state.state == :waiting_pmt && TS.Demuxer.has_pmt?(demuxer) ->
          actions =
            demuxer
            |> TS.Demuxer.take_pmts()
            |> Enum.flat_map(fn %TS.PMT{streams: streams} ->
              Enum.map(streams, fn {id, %{stream_type: type}} ->
                %{stream_id: id, stream_type: type}
              end)
            end)
            |> Enum.map(fn x -> {:notify, {:mpeg_ts_pmt_stream, x}} end)

          {actions, %{state | state: :online}}

        state.state == :waiting_pmt ->
          {[{:demand, Pad.ref(:input)}], state}

        true ->
          {[], state}
      end

    {fulfill_actions, state} = fulfill_pending_demand(state)
    {{:ok, extra_actions ++ fulfill_actions}, state}
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {:ok, %{state | closed: true}}
  end

  defp fulfill_pending_demand(state) do
    {actions, state} =
      Enum.map_reduce(state.pending_demand, state, fn {pad, _size}, state ->
        # NOTE: size is set to 1 to take advantage of the :redemand action.
        fulfill_actions_on_pad(state, pad, 1)
      end)

    actions =
      actions
      |> List.flatten()
      |> Enum.sort(fn
        {:redemand, _}, {:redemand, _} -> true
        {:redemand, _}, {_, _} -> false
        _, _ -> true
      end)

    {actions, state}
  end

  defp fulfill_actions_on_pad(state, pad = {Membrane.Pad, _, {:stream_id, sid}}, size) do
    {packets, demuxer} = TS.Demuxer.take_from_stream(state.demuxer, sid, size)
    missing = size - length(packets)
    {{pad, packets, missing}, demuxer}

    buffer_actions =
      packets
      |> Enum.map(fn {unm = MPEG.TS.PartialPES, packet} ->
        case unm.unmarshal(packet.payload, packet.is_unit_start) do
          {:ok, pes} ->
            pes

          {:error, reason} ->
            raise ArgumentError,
                  "MPEG-TS could not parse Partial PES packet: #{inspect(reason)}"
        end
      end)
      |> Enum.map(fn pes -> {:buffer, {pad, %Membrane.Buffer{payload: pes.data}}} end)

    stream_size = TS.Demuxer.stream_size(demuxer, sid)

    {actions, state} =
      cond do
        stream_size == 0 and state.closed ->
          # If the pad is not removed from the pending demand, we'll always try
          # fulfilling it again, even though end_of_stream has already been
          # reached.
          pending_demand = Map.delete(state.pending_demand, pad)
          {buffer_actions ++ [{:end_of_stream, pad}], %{state | pending_demand: pending_demand}}

        length(buffer_actions) > 0 ->
          pending_demand = Map.put(state.pending_demand, pad, missing)
          {buffer_actions ++ [{:redemand, pad}], %{state | pending_demand: pending_demand}}

        true ->
          pending_demand = Map.put(state.pending_demand, pad, missing)
          {buffer_actions, %{state | pending_demand: pending_demand}}
      end

    {actions, %{state | demuxer: demuxer}}
  end

  defp forward_demand(state) do
    actions =
      state.pending_demand
      |> Enum.map(fn {_pad, size} -> {:demand, {Pad.ref(:input), size}} end)

    {actions, state}
  end
end
