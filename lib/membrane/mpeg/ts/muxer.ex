defmodule Membrane.MPEG.TS.Muxer do
  @moduledoc """
  Experimental MPEG-TS Muxer. Supports 1 program with AAC and H264 in it only for now.

  Inputs must be attached before the element enters the playing state. Audio&Video
  are going to be interleaved by their timing.

  Each buffer is going to end in its own PES packet, hence NALu units must be grouped
  accordingly, as well as ADTS AAC frames.
  """

  use Membrane.Filter
  alias MPEG.TS

  def_input_pad(:input,
    accepted_format: _any,
    availability: :on_request,
    options: [
      stream_type: [
        spec: atom(),
        description: """
        Each input is going to become a stream in the PMT with this assigned type.
        See MPEG.TS.PMT.
        """
      ]
    ]
  )

  def_output_pad(:output,
    accepted_format: Membrane.RemoteStream
  )

  @impl true
  def handle_init(_ctx, _opts) do
    state = %{
      muxer: TS.Muxer.new(),
      pad_to_pid: %{},
      pid_to_queue: %{}
    }

    {[], state}
  end

  @impl true
  def handle_stream_format(_pad, _format, _ctx, state) do
    {[], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    format_actions = [stream_format: {:output, %Membrane.RemoteStream{}}]
    # Write PAT and PMT at startup, then periodically or at each PMT update.
    {buffer_actions, state} = mux_pat_pmt(state)
    {format_actions ++ buffer_actions, state}
  end

  @impl true
  def handle_end_of_stream({Membrane.Pad, :input, ref}, ctx, state) do
    end_of_stream? =
      ctx.pads
      |> Enum.map(fn {_ref, pad} -> pad end)
      |> Enum.filter(fn pad -> pad.direction == :input end)
      |> Enum.map(fn pad -> pad.end_of_stream? or pad == ref end)
      |> Enum.all?()

    if end_of_stream? do
      mux_and_forward_end_of_stream(state)
    else
      {[], state}
    end
  end

  @impl true
  def handle_pad_added(pad, ctx, state) do
    {pid, state} =
      get_and_update_in(state, [:muxer], fn muxer ->
        # TODO: we could indicated this stream as PCR carrier if needed.
        TS.Muxer.add_elementary_stream(muxer, ctx.pad_options[:stream_type])
      end)

    state =
      state
      |> put_in([:pad_to_pid, pad], pid)
      |> put_in([:pid_to_queue, pid], :queue.new())

    if ctx.playback == :playing do
      # Each time a pad is added at runtime, update the PMT.
      mux_pat_pmt(state)
    else
      {[], state}
    end
  end

  @impl true
  def handle_buffer(pad, buffer, _ctx, state) do
    pid = get_in(state, [:pad_to_pid, pad])

    state
    |> update_in([:pid_to_queue, pid], fn q -> :queue.in(buffer, q) end)
    |> mux_and_forward_oldest([])
  end

  defp mux_and_forward_oldest(state, acc) do
    any_empty? =
      state.pid_to_queue
      |> Enum.map(fn {_, q} -> :queue.is_empty(q) end)
      |> Enum.any?()

    if any_empty? do
      {[buffer: {:output, acc}], state}
    else
      # Find the queue containing the oldest item.
      {next_pid, _} =
        state.pid_to_queue
        |> Enum.map(fn {pid, q} ->
          {:value, x} = :queue.peek(q)
          {pid, x.pts}
        end)
        |> Enum.sort(fn {_, left}, {_, right} -> left < right end)
        |> List.first()

      {{:value, buffer}, state} =
        get_and_update_in(state, [:pid_to_queue, next_pid], fn q -> :queue.out(q) end)

      {buffers, state} = mux_and_forward(next_pid, buffer, state)
      mux_and_forward_oldest(state, acc ++ buffers)
    end
  end

  defp mux_and_forward_end_of_stream(state) do
    {buffers, state} =
      state.pid_to_queue
      |> Enum.flat_map(fn {pid, queue} ->
        queue
        |> :queue.to_list()
        |> Enum.map(fn buffer -> {pid, buffer} end)
      end)
      |> Enum.sort(fn {_, left}, {_, right} ->
        Membrane.Buffer.get_dts_or_pts(left) < Membrane.Buffer.get_dts_or_pts(right)
      end)
      |> Enum.flat_map_reduce(state, fn {pid, buffer}, state ->
        mux_and_forward(pid, buffer, state)
      end)

    state =
      update_in(state, [:pid_to_queue], fn m ->
        m
        |> Enum.map(fn {pid, _queue} -> {pid, :queue.new()} end)
        |> Map.new()
      end)

    {[buffer: {:output, buffers}, end_of_stream: :output], state}
  end

  defp ns_to_ts(nil), do: nil
  defp ns_to_ts(ns), do: round(ns * 90_000 / 1.0e9)

  defp mux_and_forward(pid, buffer, state) do
    # Isn't there a better way of doinf it?
    %{stream_type: stream_type} = state.muxer.pmt.streams[pid]
    stream_category = TS.PMT.get_stream_category(stream_type)

    keyframe? = Map.get(buffer.metadata, :is_keyframe?, false)
    sync? = keyframe? || stream_category != :video

    {packets, state} =
      get_and_update_in(state, [:muxer], fn muxer ->
        case stream_category do
          x when x in [:video, :audio] ->
            TS.Muxer.mux_sample(muxer, pid, buffer.payload, ns_to_ts(buffer.pts),
              dts: ns_to_ts(buffer.dts),
              sync?: sync?
            )

            # TODO: support PSI packets such as SCTE
        end
      end)

    buffers =
      packets
      |> Enum.map(&TS.Marshaler.marshal/1)
      |> Enum.map(&IO.iodata_to_binary/1)
      |> Enum.map(fn x ->
        %Membrane.Buffer{payload: x, pts: buffer.pts, dts: buffer.dts}
      end)
      |> then(fn
        [] ->
          []

        [h | t] ->
          h
          |> put_in([Access.key!(:metadata), :pusi], sync?)
          |> List.wrap()
          |> Enum.concat(t)
      end)

    {buffers, state}
  end

  defp mux_pat_pmt(state) do
    {pat, state} = get_and_update_in(state, [:muxer], &TS.Muxer.mux_pat(&1))
    {pmt, state} = get_and_update_in(state, [:muxer], &TS.Muxer.mux_pmt(&1))

    buffers =
      [pat, pmt]
      |> Enum.map(&TS.Marshaler.marshal/1)
      |> Enum.map(&IO.iodata_to_binary/1)
      |> Enum.map(fn x -> %Membrane.Buffer{payload: x} end)

    actions = [buffer: {:output, buffers}]
    {actions, state}
  end
end
