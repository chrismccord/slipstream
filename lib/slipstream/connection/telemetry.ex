defmodule Slipstream.Connection.Telemetry do
  @moduledoc false
  @moduledoc since: "0.3.0"

  alias Slipstream.Connection.State

  # helper functions for emitting telemetry about a connection

  @doc """
  Wraps the connection pipeline in order to emit telemetry for each message
  sent to the connection process
  """
  @doc since: "0.3.0"
  def span(initial_pipeline, func) do
    metadata =
      %{
        start_time: DateTime.utc_now(),
        span_id: Slipstream.TelemetryHelper.id(),
        connection_id: initial_pipeline.state.connection_id,
        trace_id: initial_pipeline.state.trace_id
      }
      |> put_detail(
        initial_pipeline.state,
        :raw_message,
        initial_pipeline.raw_message
      )
      |> put_detail(
        initial_pipeline.state,
        :start_state,
        initial_pipeline.state
      )

    finished_pipeline =
      :telemetry.span(
        [:slipstream, :connection, :handle],
        metadata,
        fn ->
          finished_pipeline = func.()

          metadata =
            metadata
            |> put_detail(
              finished_pipeline.state,
              :message,
              finished_pipeline.message
            )
            |> put_detail(
              finished_pipeline.state,
              :events,
              finished_pipeline.events
            )
            |> put_detail(
              finished_pipeline.state,
              :end_state,
              finished_pipeline.state
            )
            |> put_detail(
              finished_pipeline.state,
              :built_events,
              finished_pipeline.built_events
            )
            |> put_detail(
              finished_pipeline.state,
              :return,
              finished_pipeline.return
            )

          {finished_pipeline, metadata}
        end
      )

    finished_pipeline.return
  end

  @doc """
  Emits the start event for a connection
  """
  @doc since: "0.3.0"
  def begin(%State{} = state) do
    metadata =
      %{
        start_time: DateTime.utc_now(),
        start_time_monotonic: :erlang.monotonic_time(),
        connection_id: state.connection_id,
        trace_id: state.trace_id
      }
      |> put_detail(state, :state, state)

    :telemetry.execute(
      [:slipstream, :connection, :connect, :start],
      %{system_time: :erlang.system_time()},
      Map.delete(metadata, :start_time_monotonic)
    )

    metadata
  end

  @doc """
  Emits the stop event for a connection
  """
  @doc since: "0.3.0"
  def conclude(%State{} = state, reason) do
    metadata =
      state.metadata
      |> Map.delete(:start_time_monotonic)
      |> put_detail(state, :termination_reason, reason)

    :telemetry.execute(
      [:slipstream, :connection, :connect, :stop],
      %{
        duration: :erlang.monotonic_time() - state.metadata.start_time_monotonic
      },
      metadata
    )
  end

  defp put_detail(
         metadata,
         %State{config: %{telemetry_metadata: :minimal}},
         _key,
         _value
       ),
       do: metadata

  defp put_detail(metadata, _state, key, value),
    do: Map.put(metadata, key, value)
end
