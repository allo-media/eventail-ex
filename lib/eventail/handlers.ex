defmodule Eventail.Handlers do
  @moduledoc """
  Behavior for Message handlers and other bus activities.

  A handler module is registered into an Endpoint when that service is started. See `Eventail.start_link/1`.

  If an handler crashes, the message will be requeued or, if already requeued once, it will be sent to the
  dead-letter exchange if one is configured.

  All the handlers are passed a `context` map containing message context from the EDA.
  If the enpoint was provided application context, it will be
  injected into the context under the `app_context` key.
  """

  alias Eventail.Endpoint

  @doc """
  Handle received event.

  This callback is called with the following positional arguments:
    - `endpoint`: the descriptor of the endpoint that received the event.
      This endpoint can be used to log and publish events or commands.
    - `event`: the event name (routing key)
    - `payload`: the event payload as a term
    - `conversation_id`: to trace message chains
    - `context`:  a map of additional headers and properties relevant to the EDA
    (like the `timestamp` that indicates when the event entered the bus)
  """
  @callback handle_event(Endpoint.t(), String.t(), term(), String.t(), map()) :: :ok

  @doc """
  Handle received command.

  This callback is called with the following positional arguments:
    - `endpoint`: the descriptor of the endpoint that received the command.
      This endpoint can be used to log and must be used to publish the outcome of the command.
    - `command`: the command name (routing key)
    - `payload`: the command payload as a term
    - `conversation_id`: to trace message chains
    - `reply_to`: where to send the command's outcome
    - `correlation_id`: must be copied on the outcome so the sender can match it
    - `context`:  a map of additional headers and properties relevant to the EDA
    (like the `timestamp` that indicates when the event entered the bus)
  """
  @callback handle_command(
              Endpoint.t(),
              String.t(),
              term(),
              String.t(),
              String.t(),
              String.t(),
              map()
            ) :: :ok

  @doc """
  Handle received result (command outcome).

  This callback is called with the following positional arguments:
    - `endpoint`: the descriptor of the endpoint that received the result.
      This endpoint can be used to log.
    - `key`: the result routing key (that's the `reply_to` that was sent with the command)
    - `payload`: the result payload as a term
    - `conversation_id`: to trace message chains
    - `status`: the result status: `:success` if success, else `:error`
    - `correlation_id`: this tracing id is the same as the one that was set on the command this message responds to.
    - `context`:  a map of additional headers and properties relevant to the EDA
    (like the `timestamp` that indicates when the event entered the bus)
  """
  @callback handle_result(
              Endpoint.t(),
              String.t(),
              term(),
              String.t(),
              :succes | :error,
              String.t(),
              map()
            ) :: :ok
end
