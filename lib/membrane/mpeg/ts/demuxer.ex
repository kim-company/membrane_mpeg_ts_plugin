defmodule Membrane.MPEG.TS.Demuxer do
  @moduledoc """
  Demuxes MPEG TS stream.

  After transition into playing state, this element will wait for [Program
  Association Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PAT)
  and [Program Mapping
  Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PMT).
  """
  use Membrane.Filter
  require Logger

  alias MPEG.TS

  def_input_pad(:input,
    accepted_format: %Membrane.RemoteStream{},
    flow_control: :auto
  )

  def_output_pad(:output,
    availability: :on_request,
    accepted_format: %Membrane.RemoteStream{},
    flow_control: :auto,
    options: [
      pid: [
        spec: pos_integer() | nil,
        default: nil,
        description:
          "The PID of the target stream. If specified, all other selection options are ignored"
      ],
      stream_type: [
        spec: atom() | nil,
        default: nil,
        description: """
        More generic selector than pid, allows to specify the stream type of the target stream, e.g. AAC_ADTS.
        The first stream that matches the selector will be returned.
        """
      ],
      stream_category: [
        spec: atom() | nil,
        default: nil,
        description: """
        Even more generic, select either :video, :audio, :subtitles, :cues, :metadata target streams.
        The first stream that matches the selector will be returned.
        """
      ]
    ]
  )

  def_options(
    strict?: [
      spec: boolean(),
      default: false,
      desciption: "If true, demuxing errors will raise"
    ]
  )

  @impl true
  def handle_init(_ctx, opts) do
    state = %{
      demuxer: TS.Demuxer.new(strict?: opts.strict?),
      subscribers: %{},
      pid_to_pads: %{}
    }

    {[], state}
  end

  @impl true
  def handle_pad_added(pad, ctx, state) do
    state =
      state
      |> put_in([:subscribers, pad], %{opts: ctx.pad_options, bound: false})
      |> refresh_pid_to_pad()

    {[], state}
  end

  @impl true
  def handle_pad_removed(pad, _ctx, state) do
    state
    |> update_in([:subscribers], &Map.delete(&1, pad))
    |> update_in([:pid_to_pads], fn mapping ->
      mapping
      |> Enum.map(fn {pid, pads} -> {pid, Enum.filter(pads, fn x -> x != pad end)} end)
      |> Map.new()
    end)
  end

  @impl true
  def handle_buffer(:input, buffer, ctx, state) do
    {units, state} =
      get_and_update_in(state, [:demuxer], fn demuxer ->
        TS.Demuxer.demux(demuxer, buffer.payload)
      end)

    Enum.flat_map_reduce(units, state, fn unit, state ->
      handle_demuxed(unit, ctx, state)
    end)
  end

  @impl true
  def handle_end_of_stream(:input, ctx, state) do
    {units, state} =
      get_and_update_in(state, [:demuxer], fn demuxer -> TS.Demuxer.flush(demuxer) end)

    {actions, state} =
      Enum.flat_map_reduce(units, state, fn unit, state ->
        handle_demuxed(unit, ctx, state)
      end)

    eos_actions =
      Enum.filter(ctx.pads, fn
        {{Membrane.Pad, :output, _id}, _pad} ->
          true

        {_other, _pad} ->
          false
      end)
      |> Enum.map(fn {pad_name, _pad} -> {:end_of_stream, pad_name} end)

    actions = actions ++ eos_actions
    {actions, state}
  end

  defp handle_demuxed(%{payload: pmt = %TS.PMT{}}, _ctx, state) do
    actions = [notify_parent: {:pmt, pmt}]
    state = refresh_pid_to_pad(state)
    {actions, state}
  end

  defp handle_demuxed(%{pid: pid, payload: payload}, ctx, state) do
    pads = get_in(state, [:pid_to_pads, pid]) || []
    buffer = ts_unit_to_buffer(payload)

    format_actions =
      pads
      |> Enum.filter(fn pad -> ctx.pads[pad].stream_format == nil end)
      |> Enum.map(fn pad -> {:stream_format, {pad, %Membrane.RemoteStream{}}} end)

    actions = Enum.map(pads, fn pad -> {:buffer, {pad, buffer}} end)
    {format_actions ++ actions, state}
  end

  defp refresh_pid_to_pad(state) do
    avail = TS.Demuxer.available_streams(state.demuxer)

    state.subscribers
    |> Enum.filter(fn {_pad, %{bound: x}} -> not x end)
    |> Enum.reduce(state, fn {pad, %{opts: opts}}, state ->
      # Now we shall check if there is a stream in the PMT table that
      # can satisfy the stream connected stream.
      cond do
        opts[:pid] != nil ->
          # The user specified the PID directly.
          if Map.has_key?(avail, opts[:pid]), do: bind(pad, opts[:pid], state), else: state

        opts[:stream_type] != nil ->
          pid =
            Enum.find_value(avail, fn {pid, x} ->
              if x.stream_type == opts[:stream_type], do: pid
            end)

          if pid, do: bind(pad, pid, state), else: state

        opts[:stream_category] != nil ->
          pid =
            Enum.find_value(avail, fn {pid, x} ->
              x = TS.PMT.get_stream_category(x.stream_type)
              if x == opts[:stream_category], do: pid
            end)

          if pid, do: bind(pad, pid, state), else: state

        true ->
          state
      end
    end)
  end

  defp bind(pad, pid, state) do
    avail = TS.Demuxer.available_streams(state.demuxer)
    info = Map.fetch!(avail, pid)
    Membrane.Logger.info("Binding #{inspect(pad)} to #{pid} (#{inspect(info)})")

    state
    |> update_in([:pid_to_pads, pid], fn
      nil -> [pad]
      acc -> [pad | acc]
    end)
    |> put_in([:subscribers, pad, :bound], true)
  end

  defp ts_unit_to_buffer(x = %TS.PES{}) do
    %Membrane.Buffer{
      payload: x.data,
      pts: x.pts,
      dts: x.dts,
      metadata: %{
        stream_id: x.stream_id,
        is_aligned: x.is_aligned,
        discontinuity: x.discontinuity
      }
    }
  end

  defp ts_unit_to_buffer(x = %TS.PSI{}) do
    %Membrane.Buffer{
      payload: x.table,
      metadata: %{header: x.header}
    }
  end

  defp ts_unit_to_buffer(_), do: []
end
