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
    {{:ok, demand: Pad.ref(:input)}, state}
  end

  @impl true
  def handle_demand(pad, size, :buffers, _ctx, state) do
    pending = Map.update(state.pending_demand, pad, size, fn old -> old + size end)
    state = %{state | pending_demand: pending}

    maybe_fulfill_demand(state)
  end

  @impl true
  def handle_process(:input, buffer, _ctx, state = %{demuxer_agent: pid}) do
    Agent.update(pid, fn demuxer ->
      TS.Demuxer.push_buffer(demuxer, buffer.payload)
    end)

    maybe_fulfill_demand(state)
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    {:ok, %{state | closed: true}}
  end

  defp maybe_fulfill_demand(state = %{state: :waiting_pmt, demuxer_agent: pid}) do
    pmt = Agent.get(pid, fn demuxer -> TS.Demuxer.get_pmt(demuxer) end)

    if pmt != nil do
      actions = [{:notify, {:mpeg_ts_pmt, pmt}}]
      state = %{state | state: :online}
      {{:ok, actions}, state}
    else
      actions = [{:demand, Pad.ref(:input)}]
      {{:ok, actions}, state}
    end
  end

  defp maybe_fulfill_demand(state = %{state: :online, demuxer_agent: pid}) do
    # Fetch packet buffers for each pad.
    buf =
      state.pending_demand
      |> Enum.map(fn {pad = {Membrane.Pad, _, {:stream_id, sid}}, _size} ->
        packets = Agent.get_and_update(pid, fn demuxer -> TS.Demuxer.take!(demuxer, sid, 1) end)

        {pad, packets}
      end)

    # Update pending demand. Remove the pad in case we're going to send
    # :end_of_stream to it so that it is not referenced anymore.
    updated_demand =
      Enum.reduce(buf, state.pending_demand, fn {pad, packets}, acc ->
        if length(packets) == 0 and state.closed do
          Map.delete(acc, pad)
        else
          Map.update!(acc, pad, fn size -> size - length(packets) end)
        end
      end)

    # For each pad that could not be fulfilled, generate a demand action. As
    # soon as we do not know how the source pad is providing data to us (may be
    # a TS file for each demand for example), there is no strategy here on how
    # much to request. If the source has been closed and we do not have more
    # data, it means we're ready to close the pad.
    demand_or_close_actions =
      buf
      |> Enum.filter(fn {_pad, packets} -> length(packets) == 0 end)
      |> Enum.map(fn {pad, _} ->
        if state.closed do
          {:end_of_stream, pad}
        else
          {:demand, Pad.ref(:input)}
        end
      end)
      |> Enum.uniq()

    buffer_actions =
      buf
      |> Enum.filter(fn {_pad, packets} -> length(packets) > 0 end)
      |> Enum.flat_map(fn {pad, packets} ->
        Enum.map(packets, fn x -> {:buffer, {pad, %Membrane.Buffer{payload: x.data}}} end)
      end)

    actions = demand_or_close_actions ++ buffer_actions
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
