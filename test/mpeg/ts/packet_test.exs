defmodule MPEG.TS.PacketTest do
  use ExUnit.Case

  alias MPEG.TS.Packet
  alias MPEG.TS.Payload.{PSI, PES}
  alias Support.Factory

  describe "MPEG TS Packet parser" do
    test "handles valid PAT packet" do
      raw_data = Factory.pat_packet()
      assert {:ok, %Packet{payload: %PSI{table: %{1 => 4096}}}} = Packet.unmarshal(raw_data)
    end

    test "handles valid PMT packet" do
      raw_data = Factory.pmt_packet()
      assert {:ok, %Packet{payload: %PSI{table: %MPEG.TS.PMT{}}}} = Packet.unmarshal(raw_data)
    end

    test "asks for more data if packet is not complete but valid" do
      <<partial::160-binary, _rest::binary>> = Factory.pat_packet()
      assert {:error, :not_enough_data} = Packet.unmarshal(partial)
    end

    test "successfully parse a valid PES packet" do
      raw_data = Factory.data_packet_video()
      assert byte_size(raw_data) == Packet.packet_size()
      assert {:ok, %Packet{payload: %PES{data: data}}} = Packet.unmarshal(raw_data)
      assert byte_size(data) > 0
    end

    test "fails when garbage is provided" do
      data = "garbagio"
      assert {:error, :invalid_data} = Packet.unmarshal(data)
    end
  end

  # require Integer
  #
  # alias Membrane.MPEG.TS.Demuxer.Parser
  # alias Parser.State
  # alias Membrane.MPEG.TS.Support.Fixtures
  #
  # setup do
  #   [state: %Parser.State{}]
  # end
  #
  # describe "When parsing a table parser should" do
  #   test "successfully parse a valid table packet with pid in range of 0-4", %{state: state} do
  #     raw_data = Fixtures.pat_packet()
  #     assert {{:ok, data}, {"", ^state}} = Parser.parse_single_packet(raw_data, state)
  #     assert data = {0, Fixtures.pat_payload()}
  #   end
  #
  #   test "successfully parse a valid table with pid in pes range but its pid is in known_tables",
  #        %{
  #          state: state
  #        } do
  #     state = %State{state | known_tables: [0x1000]}
  #     raw_data = Fixtures.pmt_packet()
  #     assert {{:ok, data}, {"", result_state}} = Parser.parse_single_packet(raw_data, state)
  #     assert %State{state | known_tables: []} == result_state
  #     assert {4096, with_padding} = data
  #     assert String.starts_with?(with_padding, Fixtures.pmt_payload())
  #   end
  # end
  #
  # describe "When parsing a pes packet parser should" do
  #   test "successfully parse a valid pes packet", %{state: state} do
  #     expected_state = %State{state | streams: %{256 => %{started_pts_payload: :pes}}}
  #
  #     assert {{:ok, data}, {"", ^expected_state}} =
  #              Parser.parse_single_packet(Fixtures.data_packet_video(), state)
  #   end
  #
  #   test "reject an invalid packet", %{state: state} do
  #     data = "garbagio"
  #
  #     assert {{:error, :not_enough_data}, {data, state}} ==
  #              Parser.parse_single_packet(data, state)
  #   end
  #
  #   test "reject a packet that has invalid pid", %{state: state} do
  #     assert {{:error, :unsuported_stream_pid}, {"", state}} ==
  #              Fixtures.data_packet(16, "garbage")
  #              |> Parser.parse_single_packet(state)
  #   end
  #
  #   test "reject a packet with adaptation field control 0b10", %{state: state} do
  #     assert {{:error, {:unsupported_packet, :only_adaptation_field}}, {"", result_state}} =
  #              Fixtures.data_packet(4000, "garbage", adaptation_field_control: 0b10)
  #              |> Parser.parse_single_packet(state)
  #
  #     assert %State{
  #              state
  #              | streams: %{4000 => %{started_pts_payload: nil}}
  #            } == result_state
  #   end
  #
  #   test "reject a packet with adaptation field control set to reserved", %{state: state} do
  #     assert {{:error, {:invalid_packet, :adaptation_field_control}}, {"", result_state}} =
  #              Fixtures.data_packet(4000, "garbage", adaptation_field_control: 0b00)
  #              |> Parser.parse_single_packet(state)
  #
  #     assert %State{
  #              state
  #              | streams: %{4000 => %{started_pts_payload: nil}}
  #            } == result_state
  #   end
  #
  #   test "reject a packet with adaptation field control 0b11 with invalid format", %{state: state} do
  #     assert {{:error, {:invalid_packet, :adaptation_field}}, {"", result_state}} =
  #              Fixtures.data_packet(4096, "payload", adaptation_size: 1000)
  #              |> Parser.parse_single_packet(state)
  #
  #     assert result_state == %State{state | streams: %{4096 => %{started_pts_payload: nil}}}
  #   end
  #
  #   test "reject a packet that contains no pes data", %{state: state} do
  #     assert {{:error, {:unsupported_packet, :not_pes}}, {"", result_state}} =
  #              Fixtures.data_packet(4096, "payload", pes_data: <<2::24>>)
  #              |> Parser.parse_single_packet(state)
  #
  #     assert result_state == %State{state | streams: %{4096 => %{started_pts_payload: nil}}}
  #   end
  #
  #   test "reject a packet that contains invalid optional pes", %{state: state} do
  #     expected_state = %State{state | streams: %{4096 => %{started_pts_payload: nil}}}
  #     payload = "p"
  #     optional_indicator = <<0b10::2, 0::6>>
  #
  #     assert {{:error, {:invalid_packet, :pes_optional}}, {"", ^expected_state}} =
  #              Fixtures.data_packet(4096, optional_indicator <> payload)
  #              |> Parser.parse_single_packet(state)
  #   end
  #
  #   test "successfully parses a valid pes packet without optional fields", %{state: state} do
  #     expected_state = %State{state | streams: %{4096 => %{started_pts_payload: :pes}}}
  #     payload = "payload"
  #
  #     assert {{:ok, {4096, data}}, {"", ^expected_state}} =
  #              Fixtures.data_packet(4096, payload)
  #              |> Parser.parse_single_packet(state)
  #
  #     assert String.starts_with?(data, payload)
  #   end
  # end
  #
  # describe "When parsing multiple pes packets parser should" do
  #   test "parse all valid packets and return their contents", %{state: state} do
  #     payload_base = "payload"
  #     pids = 256..260
  #
  #     data =
  #       pids
  #       |> Enum.map(fn pid ->
  #         Fixtures.data_packet(pid, payload_base <> to_string(pid))
  #       end)
  #       |> Enum.join()
  #
  #     assert {result, "", %State{streams: streams}} = Parser.parse_packets(data, state)
  #
  #     assert pids
  #            |> Enum.zip(result)
  #            |> Enum.all?(fn {pid, {s_pid, stream}} ->
  #              pid == s_pid && String.starts_with?(stream, payload_base <> to_string(pid))
  #            end)
  #
  #     assert pids
  #            |> Enum.zip(streams)
  #            |> Enum.all?(fn {pid, {s_pid, stream}} ->
  #              pid == s_pid && stream == %{started_pts_payload: :pes}
  #            end)
  #   end
  #
  #   test "parse mix of valid and invalid packets and return valid packets", %{state: state} do
  #     pids = 256..260
  #     payload_base = "payload"
  #
  #     data =
  #       pids
  #       |> Enum.map(fn pid ->
  #         adaptation_field_control =
  #           if Integer.is_even(pid) do
  #             3
  #           else
  #             0
  #           end
  #
  #         Fixtures.data_packet(pid, payload_base <> to_string(pid),
  #           adaptation_field_control: adaptation_field_control
  #         )
  #       end)
  #       |> Enum.join()
  #
  #     assert {result, "", %State{streams: streams}} = Parser.parse_packets(data, state)
  #     assert result |> Map.keys() |> length() == 3
  #   end
  # end
end
