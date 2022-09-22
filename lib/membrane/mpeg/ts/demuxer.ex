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
  require Membrane.Logger

  alias MPEG.TS

  def_input_pad(:input, caps: :any, demand_unit: :buffers, demand_mode: :manual)
  def_output_pad(:output, availability: :on_request, caps: :any)

  @type state_t :: :waiting_pmt | :online

  @impl true
  def handle_init(_) do
    {:ok, new_state()}
  end

  @impl true
  def handle_pad_added(pad = {Membrane.Pad, _, {:stream_id, _sid}}, _, state) do
    # TODO: as soon as we've parse the PMT at this point, we can actually tell
    # something abount the data format (see PMT stream_type). Replace :any with
    # that information ASAP.
    {{:ok, [{:caps, {pad, :any}}]}, state}
  end

  @impl true
  def handle_prepared_to_stopped(_ctx, %{demuxer_agent: pid}) do
    Agent.stop(pid, :normal)
    {:ok, new_state()}
  end

  @impl true
  def handle_prepared_to_playing(_ctx, state) do
    # Output pad is connected after a PMT table is recognized, hence we have to
    # ask for the first packets by ourselves.
    {{:ok, demand: {Pad.ref(:input), 1}}, state}
  end

  @impl true
  def handle_demand(pad, size, :buffers, _ctx, state) do
    pending = Map.update(state.pending_demand, pad, size, fn old -> old + size end)
    state = %{state | pending_demand: pending}

    # If buffers are sent too early (e.g. by always calling fulfill_demand
    # here), buffers will be re-ordered on the output, as if we where supplying
    # in a moment where the pipeline is not ready to accept it. To see the
    # behaviour, check commit 7d1389521763f05b2ebbe921f4382e791daf0a6c.
    #
    # On the other hand we have to fulfill demand on newly added pads here if
    # the stream has been closed as chances are handle_process won't be called
    # anymore as all input demand has already been processed.
    if state.closed do
      fulfill_demand(state)
    else
      {{:ok, demand: {Pad.ref(:input), 1}}, state}
    end
  end

  @impl true
  def handle_process(:input, buffer, _ctx, state = %{demuxer_agent: pid}) do
    Agent.update(pid, fn demuxer ->
      TS.Demuxer.push_buffer(demuxer, buffer.payload)
    end)

    fulfill_demand(state)
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {:ok, %{state | closed: true}}
  end

  defp fulfill_demand(state = %{state: :waiting_pmt, demuxer_agent: pid}) do
    # Eventually I would prefer to drop the state differentiation and notify
    # the pipeline each time a different PMT table is parsed.
    pmt = Agent.get(pid, fn demuxer -> TS.Demuxer.get_pmt(demuxer) end)

    if pmt != nil do
      actions = [{:notify, {:mpeg_ts_pmt, pmt}}]
      state = %{state | state: :online}
      {{:ok, actions}, state}
    else
      actions = [{:demand, {Pad.ref(:input), 1}}]
      {{:ok, actions}, state}
    end
  end

  defp fulfill_demand(state = %{state: :online, demuxer_agent: pid}) do
    # Fetch packet buffers for each pad.
    buf =
      state.pending_demand
      |> Enum.map(fn {pad = {Membrane.Pad, _, {:stream_id, sid}}, size} ->
        packets = Agent.get_and_update(pid, fn demuxer -> TS.Demuxer.take(demuxer, sid, size) end)

        left = Agent.get(pid, fn demuxer -> TS.Demuxer.stream_size(demuxer, sid) end)

        {pad, packets, left}
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
        [{:demand, {Pad.ref(:input), 1}}]
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
            %Membrane.Buffer{
              payload: x.data,
              metadata: %{
                stream_id: sid
              }
            }
          end)}}
      end)

    actions = buffer_actions ++ demand_or_close_actions
    state = %{state | pending_demand: updated_demand}

    {{:ok, actions}, state}
  end

  defp new_state() do
    {:ok, pid} = Agent.start_link(&TS.Demuxer.new/0)

    %{
      state: :waiting_pmt,
      demuxer_agent: pid,
      pending_demand: %{},
      closed: false
    }
  end
end
