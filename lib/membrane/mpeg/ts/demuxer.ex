defmodule Membrane.MPEG.TS.Demuxer do
  @moduledoc """
  Demuxes MPEG TS stream.

  After transition into playing state, this element will wait for [Program
  Association Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PAT)
  and [Program Mapping
  Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PMT).

  For `:H264_AVC` streams, the demuxer emits `%Membrane.H264{alignment: :au,
  stream_structure: :annexb}` and assumes each PES contains exactly one access
  unit.

  Use `profile:` to select streams that require descriptors and depacketization
  (e.g. Opus in MPEG-TS). For custom payloads, match `stream_type:` and
  `registration_descriptor:` explicitly.
  """
  use Membrane.Filter
  require Membrane.Logger

  alias MPEG.TS
  alias MPEG.TS.Demuxer.Container

  def_input_pad(:input,
    accepted_format: %Membrane.RemoteStream{},
    flow_control: :auto
  )

  def_output_pad(:output,
    availability: :on_request,
    accepted_format: any_of(Membrane.RemoteStream, Membrane.H264),
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
      profile: [
        spec: atom() | nil,
        default: nil,
        description: "Well-known stream profile (e.g. :opus_mpeg_ts, :scte35)."
      ],
      registration_descriptor: [
        spec: String.t() | nil,
        default: nil,
        description: """
        Matches a registration descriptor (tag 0x05) format identifier, e.g. "Opus".
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
    ],
    wait_rai?: [
      spec: boolean(),
      default: true,
      description: "If true, first packet buffer is going to be a keyframe (in case of video)"
    ],
    drop_until_video_rai?: [
      spec: boolean(),
      default: false,
      description:
        "If true, drop all non-video buffers until the first video buffer (RAI) arrives"
    ]
  )

  @impl true
  def handle_init(_ctx, opts) do
    state = %{
      demuxer:
        TS.Demuxer.new(
          strict?: Map.get(opts, :strict?, false),
          wait_rai?: Map.get(opts, :wait_rai?, true)
        ),
      subscribers: %{},
      pid_to_pads: %{},
      pid_to_stream_category: %{},
      has_video_stream?: false,
      drop_until_video_rai?: Map.get(opts, :drop_until_video_rai?, false),
      video_started?: false
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
    stream = state.demuxer |> TS.Demuxer.available_streams() |> Map.get(pid)
    buffer = ts_unit_to_buffer(unit, stream)
    discontinue? = get_in(buffer, [Access.key!(:metadata), Access.key(:discontinuity, false)])

    {actions, state} =
      if drop_until_video?(state, pid, stream) do
        {[], state}
      else
        state =
          if video_stream?(state, pid, stream) do
            %{state | video_started?: true}
          else
            state
          end

        actions =
          Enum.flat_map(pads, fn pad ->
            extras =
              if discontinue?, do: [{:event, {pad, %Membrane.Event.Discontinuity{}}}], else: []

            extras ++ [{:buffer, {pad, buffer}}]
          end)

        {actions, state}
      end

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

        opts[:profile] != nil ->
          pid =
            Enum.find_value(avail, fn {pid, x} ->
              if profile_match?(x, opts[:profile]), do: pid
            end)

          if pid, do: bind(pad, pid, state), else: state

        opts[:stream_type] != nil ->
          pid =
            Enum.find_value(avail, fn {pid, x} ->
              if x.stream_type == opts[:stream_type] and
                   registration_descriptor_match?(x, opts[:registration_descriptor]) do
                pid
              end
            end)

          if pid, do: bind(pad, pid, state), else: state

        opts[:stream_category] != nil ->
          pid =
            Enum.find_value(avail, fn {pid, x} ->
              if stream_category_for(x) == opts[:stream_category] and
                   registration_descriptor_match?(x, opts[:registration_descriptor]) do
                pid
              end
            end)

          if pid, do: bind(pad, pid, state), else: state

        opts[:registration_descriptor] != nil ->
          pid =
            Enum.find_value(avail, fn {pid, x} ->
              if registration_descriptor_match?(x, opts[:registration_descriptor]), do: pid
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

    category = TS.PMT.get_stream_category(info.stream_type)

    state
    |> update_in([:pid_to_pads, pid], fn
      nil -> [pad]
      acc -> [pad | acc]
    end)
    |> put_in([:subscribers, pad, :bound], true)
    |> update_in([:pid_to_stream_category], &Map.put(&1, pid, category))
    |> update_in([:has_video_stream?], &(&1 or category == :video))
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

          format =
            case stream.stream_type do
              :H264_AVC ->
                %Membrane.H264{alignment: :au, stream_structure: :annexb}

              _other ->
                %Membrane.RemoteStream{
                  content_format: %Membrane.MPEG.TS.StreamFormat{
                    stream_type: stream.stream_type,
                    descriptors: stream.descriptors
                  }
                }
            end

          {:stream_format, {pad, format}}
        end)
      end)

    {actions, state}
  end

  defp ts_unit_to_buffer(%Container{payload: x = %TS.PES{}}, stream) do
    payload = maybe_depacketize_opus(x.data, stream)

    %Membrane.Buffer{
      payload: payload,
      pts: x.pts,
      dts: x.dts,
      metadata: %{
        stream_id: x.stream_id,
        is_aligned: x.is_aligned,
        discontinuity: x.discontinuity
      }
    }
  end

  defp ts_unit_to_buffer(%Container{payload: x = %TS.PSI{}, t: t}, _stream) do
    %Membrane.Buffer{
      payload: TS.Marshaler.marshal(x),
      dts: t,
      metadata: %{
        psi: x
      }
    }
  end

  defp registration_descriptor_match?(_stream, nil), do: true

  defp registration_descriptor_match?(%{descriptors: descriptors}, format_identifier) do
    Enum.any?(descriptors, fn
      %{tag: 0x05, data: ^format_identifier} -> true
      _ -> false
    end)
  end

  defp registration_descriptor_match?(_stream, _format_identifier), do: false

  defp drop_until_video?(%{drop_until_video_rai?: false}, _pid, _stream), do: false

  defp drop_until_video?(%{video_started?: true}, _pid, _stream), do: false

  defp drop_until_video?(state, pid, stream) do
    state.has_video_stream? and not video_stream?(state, pid, stream)
  end

  defp video_stream?(_state, _pid, nil), do: false

  defp video_stream?(state, pid, stream) do
    Map.get(state.pid_to_stream_category, pid, TS.PMT.get_stream_category(stream.stream_type)) ==
      :video
  end

  defp stream_category_for(stream) do
    case resolve_profile_for_stream(stream) do
      %{stream_category: category} -> category
      _ -> TS.PMT.get_stream_category(stream.stream_type)
    end
  end

  defp resolve_profile_for_stream(stream) do
    Enum.find_value(Membrane.MPEG.TS.Profiles.list(), fn profile ->
      profile_data = Membrane.MPEG.TS.Profiles.fetch!(profile)

      if stream.stream_type == profile_data.stream_type and
           descriptors_match?(stream.descriptors, profile_data.descriptors) do
        profile_data
      else
        false
      end
    end)
  end

  defp profile_match?(stream, profile) do
    profile_data = Membrane.MPEG.TS.Profiles.fetch!(profile)

    stream.stream_type == profile_data.stream_type and
      descriptors_match?(stream.descriptors, profile_data.descriptors)
  end

  defp descriptors_match?(_stream_descriptors, nil), do: true

  defp descriptors_match?(stream_descriptors, required_descriptors) do
    Enum.all?(required_descriptors, fn required ->
      Enum.any?(stream_descriptors, &(&1 == required))
    end)
  end

  defp maybe_depacketize_opus(payload, stream) do
    case resolve_profile_for_stream(stream) do
      %{packetizer: nil} ->
        payload

      %{packetizer: packetizer} ->
        case packetizer.depacketize(payload) do
          {:ok, raw} ->
            raw

          {:error, reason} ->
            Membrane.Logger.warning("Failed to depacketize payload: #{inspect(reason)}")
            payload
        end

      _ ->
        payload
    end
  end
end
