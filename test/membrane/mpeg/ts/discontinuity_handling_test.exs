defmodule Membrane.MPEG.TS.DiscontinuityHandlingTest do
  use ExUnit.Case

  alias Membrane.Buffer
  alias Membrane.Testing
  import Membrane.Testing.Assertions
  require Membrane.Pad, as: Pad

  @input_ts_file "test/fixtures/bundestag/all.ts"

  defmodule TestingSink do
    use Membrane.Sink

    def_input_pad(:input, accepted_format: _any, flow_control: :auto)

    @impl true
    def handle_init(_ctx, _opts) do
      {[], %{fsm_state: :before_discontinuity}}
    end

    @impl true
    def handle_event(:input, %Membrane.Event.Discontinuity{}, _ctx, state) do
      {[], %{state | fsm_state: :after_discontinuity}}
    end

    @impl true
    def handle_buffer(
          :input,
          %Buffer{metadata: %{discontinuity: false}},
          _ctx,
          %{fsm_state: :before_discontinuity} = state
        ) do
      {[], state}
    end

    @impl true
    def handle_buffer(
          :input,
          %Buffer{metadata: %{discontinuity: true}},
          _ctx,
          %{fsm_state: :after_discontinuity} = state
        ) do
      {[], state}
    end

    @impl true
    def handle_buffer(:input, _buffer, _ctx, _state) do
      raise "Discontinuity event is dysynchronized with the buffers!"
    end

    @impl true
    def handle_end_of_stream(:input, _ctx, state) do
      {[notify_parent: :eos], state}
    end
  end

  test "synchronizes events with buffers" do
    import Membrane.ChildrenSpec

    spec =
      child(:source, %Testing.Source{
        output: {:first_ts, &generator/2}
      })
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer)

    pid = Testing.Pipeline.start_link_supervised!(spec: spec)

    receive do
      {Membrane.Testing.Pipeline, _pid,
       {:handle_child_notification, {{:mpeg_ts_pmt, pmt}, :demuxer}}} ->
        pmt.streams
    end

    link =
      get_child(:demuxer)
      |> via_out(Pad.ref(:output, {:stream_id, 481}))
      |> child(:sink, TestingSink)

    Testing.Pipeline.execute_actions(pid, spec: link)
    assert_pipeline_notified(pid, :sink, :eos)
    Testing.Pipeline.terminate(pid)
  end

  defp generator(:first_ts, _size) do
    payload = File.read!(@input_ts_file)

    {[
       stream_format: {:output, %Membrane.RemoteStream{}},
       buffer: {:output, %Buffer{payload: payload, metadata: %{discontinuity: false}}},
       redemand: :output
     ], :second_ts}
  end

  defp generator(:second_ts, _size) do
    payload = File.read!(@input_ts_file)

    {[
       buffer: {:output, %Buffer{payload: payload, metadata: %{discontinuity: true}}},
       end_of_stream: :output
     ], :finished}
  end
end
