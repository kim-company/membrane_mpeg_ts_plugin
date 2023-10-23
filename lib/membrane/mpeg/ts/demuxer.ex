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
  require Logger

  alias MPEG.TS

  @h264_time_base 90_000

  # Amount of seconds after which the demuxer will start filtering out streams
  # that are not being followed.
  @stream_filter_timeout 5_000

  def_input_pad(:input,
    accepted_format: %Membrane.RemoteStream{},
    demand_unit: :buffers,
    demand_mode: :manual
  )

  def_output_pad(:output,
    availability: :on_request,
    accepted_format: %module{} when module in [Membrane.RemoteStream, Membrane.H264],
    options: [
      is_aligned: [
        spec: boolean(),
        default: true,
        description: """
        Determines if the output sent from this pad is aligned.
        If set to true, it will be assumed that each PES packet sent from
        this pad contains a complete stream unit (e.g. complete Access Unit in
        case of H264 stream).
        Otherwise, no such an assumption will be made.
        This option affects the stream format sent through this pad.
        """
      ]
    ]
  )

  @type state_t :: :waiting_pmt | :online

  @impl true
  def handle_init(_ctx, _opts) do
    {[], new_state()}
  end

  @impl true
  def handle_pad_added(pad = {Membrane.Pad, _, {:stream_id, sid}}, ctx, state) do
    is_aligned = ctx.options[:is_aligned]

    format =
      cond do
        Map.fetch!(state.demuxer.pmt.streams, sid)[:stream_type] == :H264 and is_aligned ->
          %Membrane.H264{alignment: :au}

        true ->
          %Membrane.RemoteStream{}
      end

    state = put_in(state, [:pads_alignments, sid], is_aligned)
    {[stream_format: {pad, format}], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    # Output pad is connected after a PMT table is recognized, hence we have to
    # ask for the first packets by ourselves.
    {[demand: Pad.ref(:input)], state}
  end

  @impl true
  def handle_demand(pad, size, :buffers, _ctx, state) do
    pending = Map.update(state.pending_demand, pad, size, fn old -> old + size end)
    fulfill_demand(%{state | pending_demand: pending})
  end

  @impl true
  def handle_process(:input, buffer, _ctx, state) do
    state
    |> update_in([:demuxer], &TS.Demuxer.push_buffer(&1, buffer.payload))
    |> fulfill_demand()
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    demuxer = TS.Demuxer.end_of_stream(state.demuxer)
    fulfill_demand(%{state | closed: true, demuxer: demuxer})
  end

  @impl true
  def handle_info(
        :start_stream_filter,
        _ctx,
        state = %{pending_demand: demand}
      ) do
    # Remove unfollowed tracks.
    followed_stream_ids = Enum.map(demand, fn {{_, _, {:stream_id, sid}}, _} -> sid end)

    demuxer = %TS.Demuxer{state.demuxer | packet_filter: &(&1 in followed_stream_ids)}

    Logger.warning(
      "PES filtering enabled. Following streams #{inspect(followed_stream_ids)}",
      domain: __MODULE__
    )

    {[], %{state | demuxer: demuxer}}
  end

  defp fulfill_demand(state = %{state: :waiting_pmt}) do
    # Eventually I would prefer to drop the state differentiation and notify
    # the pipeline each time a different PMT table is parsed.
    pmt = state.demuxer.pmt

    if pmt != nil do
      Process.send_after(self(), :start_stream_filter, @stream_filter_timeout)
      actions = [{:notify_parent, {:mpeg_ts_pmt, pmt}}]
      state = %{state | state: :online}
      {actions, state}
    else
      actions = [{:demand, Pad.ref(:input)}]
      {actions, state}
    end
  end

  defp fulfill_demand(state = %{state: :online, demuxer: demuxer}) do
    # Fetch packet buffers for each pad.
    {buf, demuxer} =
      state.pending_demand
      |> Enum.map_reduce(demuxer, fn {pad = {_, _, {:stream_id, sid}}, size}, demuxer ->
        {packets, demuxer} = TS.Demuxer.take(demuxer, sid, size)
        left = TS.Demuxer.size(demuxer, sid)

        {{pad, packets, left}, demuxer}
      end)

    # Given the buffers obtained for each pad, update their demand.
    updated_demand =
      Enum.reduce(buf, state.pending_demand, fn {pad, packets, _}, acc ->
        Map.update!(acc, pad, fn size -> size - length(packets) end)
      end)

    # For each pad that could not be fulfilled, generate a demand action. As
    # soon as we do not know how the source pad is providing data to us (may be
    # a TS file for each demand for example), there is no strategy here on how
    # much to request. If the source has been closed and we do not have more
    # data, it means we're ready to close the pad.
    demand_or_close_actions =
      if state.closed do
        buf
        |> Enum.filter(fn {_pad, _packets, left} -> left == 0 end)
        |> Enum.filter(fn {pad, _packets, _} -> Map.has_key?(updated_demand, pad) end)
        |> Enum.map(fn {pad, _, _} -> {:end_of_stream, pad} end)
      else
        [{:demand, Pad.ref(:input)}]
      end

    # Now we know which pads will received an :end_of_stream action. Remove
    # them from the pending_demand map to avoid sending messages to these pads
    # again.
    updated_demand =
      demand_or_close_actions
      |> Enum.filter(fn {action, _} -> action == :end_of_stream end)
      |> Enum.reduce(state.pending_demand, fn {_, pad}, acc ->
        Map.delete(acc, pad)
      end)

    buffer_actions =
      buf
      |> Enum.filter(fn {_pad, packets, _} -> length(packets) > 0 end)
      |> Enum.map(fn {pad = {Membrane.Pad, _, {:stream_id, sid}}, packets, _} ->
        {:buffer,
         {pad,
          Enum.map(packets, fn x ->
            if state.pads_alignments[sid] and not x.is_aligned do
              Logger.warning("""
              You have specified that the stream for pad #{inspect(pad)} is aligned,
              but the PES packets have `alignment_indicator` value meaning that they are not aligned.
              Consider setting `is_aligned: false` output pad option.
              """)
            end

            %Membrane.Buffer{
              payload: x.data,
              pts: parse_pts_or_dts(x.pts),
              dts: parse_pts_or_dts(x.dts),
              metadata: %{
                stream_id: sid
              }
            }
          end)}}
      end)

    actions = buffer_actions ++ demand_or_close_actions
    state = %{state | pending_demand: updated_demand, demuxer: demuxer}

    {actions, state}
  end

  defp parse_pts_or_dts(nil), do: nil

  defp parse_pts_or_dts(ts) do
    use Ratio
    (ts * Membrane.Time.second() / @h264_time_base) |> Ratio.trunc()
  end

  defp new_state() do
    %{
      state: :waiting_pmt,
      demuxer: TS.Demuxer.new(),
      pending_demand: %{},
      closed: false,
      pads_alignments: %{}
    }
  end
end
