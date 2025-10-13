defmodule Membrane.MPEG.TS.Demuxer do
  @moduledoc """
  Demuxes MPEG TS stream.

  After transition into playing state, this element will wait for [Program
  Association Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PAT)
  and [Program Mapping
  Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PMT).
  """
  use Membrane.Filter
  require Membrane.Logger

  alias MPEG.TS
  alias MPEG.TS.Demuxer.Container

  # MPEG-TS 33-bit rollover period in nanoseconds: 2^33 * (10^9 / 90000)
  @rollover_period_ns 95_443_717_688_889
  @rollover_threshold div(@rollover_period_ns, 2)

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
      pid_to_pads: %{},
      rollover: %{}
    }

    {[], state}
  end

  @impl true
  def handle_stream_format(_pad, _format, _ctx, state) do
    {[], state}
  end

  @impl true
  def handle_pad_added(pad, ctx, state) do
    state =
      state
      |> put_in([:subscribers, pad], %{opts: ctx.pad_options, bound: false})
      |> refresh_pid_to_pad()

    if ctx.playback == :playing do
      maybe_forward_stream_format(ctx, state)
    else
      {[], state}
    end
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

  defp handle_demuxed(%Container{payload: %TS.PSI{table_type: type, table: table}}, ctx, state)
       when type in [:pmt, :pat] do
    actions = [notify_parent: {type, table}]
    state = if type == :pmt, do: refresh_pid_to_pad(state), else: state
    {format_actions, state} = maybe_forward_stream_format(ctx, state)
    {actions ++ format_actions, state}
  end

  defp handle_demuxed(unit = %Container{pid: pid}, _ctx, state) do
    pads = get_in(state, [:pid_to_pads, Access.key(pid, [])])
    {buffer, updated_rollover} = ts_unit_to_buffer(unit, pid, state.rollover)
    discontinue? = get_in(buffer, [Access.key!(:metadata), Access.key(:discontinuity, false)])

    actions =
      Enum.flat_map(pads, fn pad ->
        extras = if discontinue?, do: [{:event, {pad, %Membrane.Event.Discontinuity{}}}], else: []
        extras ++ [{:buffer, {pad, buffer}}]
      end)

    state = %{state | rollover: updated_rollover}

    {actions, state}
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
    Membrane.Logger.info("Binding #{inspect(pad)} to #{pid} (#{inspect(info.stream_type)})")

    state
    |> update_in([:pid_to_pads, pid], fn
      nil -> [pad]
      acc -> [pad | acc]
    end)
    |> put_in([:subscribers, pad, :bound], true)
  end

  defp maybe_forward_stream_format(ctx, state) do
    actions =
      state.pid_to_pads
      |> Enum.flat_map(fn {pid, pads} ->
        pads
        |> Enum.filter(fn pad -> ctx.pads[pad].stream_format == nil end)
        |> Enum.map(fn pad ->
          stream =
            state.demuxer
            |> TS.Demuxer.available_streams()
            |> Map.fetch!(pid)

          format = %Membrane.RemoteStream{
            content_format: %Membrane.MPEG.TS.StreamFormat{stream_type: stream.stream_type}
          }

          {:stream_format, {pad, format}}
        end)
      end)

    {actions, state}
  end

  defp ts_unit_to_buffer(%Container{payload: x = %TS.PES{}, pid: pid}, pid, rollover) do
    pid_rollover = Map.get(rollover, pid, %{pts: %{}, dts: %{}})
    {corrected_pts, updated_pts} = correct_timestamp("#{pid}:pts", x.pts, pid_rollover.pts)
    {corrected_dts, updated_dts} = correct_timestamp("#{pid}:dts", x.dts, pid_rollover.dts)

    buffer = %Membrane.Buffer{
      payload: x.data,
      pts: corrected_pts,
      dts: corrected_dts,
      metadata: %{
        stream_id: x.stream_id,
        is_aligned: x.is_aligned,
        discontinuity: x.discontinuity,
        original_pts: x.pts,
        original_dts: x.dts
      }
    }

    updated_rollover = Map.put(rollover, pid, %{pts: updated_pts, dts: updated_dts})
    {buffer, updated_rollover}
  end

  defp ts_unit_to_buffer(
         %Container{payload: x = %TS.PSI{}, t: best_effort_t, pid: pid},
         pid,
         rollover
       ) do
    pid_rollover = Map.get(rollover, pid, %{pts: %{}})

    {corrected_pts, updated_pts} =
      correct_timestamp("#{pid}:pts", best_effort_t, pid_rollover.pts)

    buffer = %Membrane.Buffer{
      payload: TS.Marshaler.marshal(x),
      pts: corrected_pts,
      metadata: %{
        psi: x,
        original_pts: best_effort_t
      }
    }

    updated_rollover = Map.put(rollover, pid, %{pts: updated_pts, dts: pid_rollover.dts})
    {buffer, updated_rollover}
  end

  defp correct_timestamp(_tag, nil, ts_state) do
    {nil, ts_state}
  end

  defp correct_timestamp(tag, timestamp, ts_state) when ts_state != %{} do
    %{last: last_ts, count: count} = ts_state

    cond do
      last_ts - timestamp > @rollover_threshold ->
        Membrane.Logger.info(
          "[#{tag}] Rollover occured from #{timestamp} to #{last_ts}. Increasing rollover count #{count + 1}."
        )

        new_count = count + 1
        corrected_ts = timestamp + new_count * @rollover_period_ns
        {corrected_ts, %{last: timestamp, count: new_count}}

      timestamp - last_ts > @rollover_threshold and count > 0 ->
        Membrane.Logger.info(
          "[#{tag}] Rollover occured from #{timestamp} to #{last_ts}. Decreasing rollover count to #{count - 1}."
        )

        new_count = count - 1
        corrected_ts = timestamp + new_count * @rollover_period_ns
        {corrected_ts, %{last: timestamp, count: new_count}}

      true ->
        corrected_ts = timestamp + count * @rollover_period_ns
        {corrected_ts, %{last: timestamp, count: count}}
    end
  end

  defp correct_timestamp(_tag, timestamp, _ts_state) do
    {timestamp, %{last: timestamp, count: 0}}
  end
end
