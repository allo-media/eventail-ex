defmodule Eventail.Handlers do
  @moduledoc """
  Behavior for Message handlers and other bus activities.

  A handler module is registered into an Endpoint when that service is started. See `Eventail.start_link/1`.

  If a business (event or command) handler crashes, the message will be requeued or, if already requeued once, it will be sent to the
  dead-letter exchange if one is configured.

  If a configuration handler crashes, the endpoint crashes.

  All the handlers are passed a `context` map containing message context from the EDA.
  If the endpoint was provided application context, it will be
  injected into the context under the `app_context` key.

  The application context can be modified by configuration handlers only.

  On startup, if configuration subscriptions are defined, the endpoint only subscribes to the configuration bus,
  and it emits the local `:waiting_for_config` event. The application can then react by publishing one or more
  requests for configuration on the configuration bus. Once the configuration data are received, the configuration
  handlers can tell the endpoint to fully subscribe to the other buses by returning a special value.
  The endpoint then finishes joining the EDA and emits the local event `:ready` when it has done so.
  See `c:handle_config/4`.
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

  Return value:
    - `{:ok, new_app_context}` to indicate all processing is done (the processed message will be acknowledged to the broker)
    - `{:manual, new_app_context}`: some processing is still on-going, the message will be manually acknowledged (or rejected) later.

  In case of `:manual`, the message must be ACK'ed or NACK'ed at some point later otherwise the enpoint won't be
  fed by the broker anymore.

  Messages are manually ACk'ed with `Eventail.Business.ack/3`, or NACK'ed with `Eventail.Business.nack/3`.

  `new_app_context` is the updated application context (not the whole context! so there is no `app_context` key in there).
  """
  @callback handle_event(
              endpoint :: Endpoint.t(),
              event :: String.t(),
              payload :: any(),
              conversation_id :: String.t(),
              context :: map()
            ) :: {:ok, map()} | {:manual, map()}

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

  Return value:
    - `{:ok, new_app_context}` to indicate all processing is done (the processed message will be acknowledged to the broker)
    - `{:manual, new_app_context}`: some processing is still on-going, the message will be manually acknowledged (or rejected) later.

  In case of `:manual`, the message must be ACK'ed or NACK'ed at some point later otherwise the enpoint won't be
  fed by the broker anymore.

  Messages are manually ACk'ed with `Eventail.Business.ack/3`, or NACK'ed with `Eventail.Business.nack/3`.

  `new_app_context` is the updated application context (not the whole context! so there is no `app_context` key in there).
  """
  @callback handle_command(
              endpoint :: Endpoint.t(),
              command :: String.t(),
              payload :: any(),
              conversation_id :: String.t(),
              reply_to :: String.t(),
              correlation_id :: String.t(),
              context :: map()
            ) :: {:ok, map()} | {:manual, map()}

  @doc """
  Handle received result (command outcome).

  This callback is called with the following positional arguments:
    - `endpoint`: the descriptor of the endpoint that received the result.
      This endpoint can be used to log.
    - `key`: the result routing key (that's the `reply_to` that was sent with the command)
    - `payload`: the result payload as a term
    - `conversation_id`: to trace message chains
    - `status`: the result status: `"success"` if success, else `"error"`
    - `correlation_id`: this tracing id is the same as the one that was set on the command this message responds to.
    - `context`:  a map of additional headers and properties relevant to the EDA
    (like the `timestamp` that indicates when the event entered the bus and `app_context`, the application context)

  Return value:
    - `{:ok, new_app_context}` to indicate all processing is done (the processed message will be acknowledged to the broker)
    - `{:manual, new_app_context}`: some processing is still on-going, the message will be manually acknowledged (or rejected) later.

  In case of `:manual`, the message must be ACK'ed or NACK'ed at some point later otherwise the enpoint won't be
  fed by the broker anymore.

  Messages are manually ACk'ed with `Eventail.Business.ack/3`, or NACK'ed with `Eventail.Business.nack/3`.

  `new_app_context` is the updated application context (not the whole context! so there is no `app_context` key in there).
  """
  @callback handle_result(
              endpoint :: Endpoint.t(),
              key :: String.t(),
              payload :: any(),
              conversation_id :: String.t(),
              status :: String.t(),
              correlation_id :: String.t(),
              context :: map()
            ) :: {:ok, map()} | {:manual, map()}

  @doc """
  Handle local events.

  Local events are internal to the application.
  Some are defined by Eventail, and others are custom and can be emitted with the `Eventail.publish_local/3`.
  There are no subscriptions to local events, this handler receives all the local events of its associated
  endpoint.

  This callback is called with the following positional arguments:
    - `endpoint`: the descriptor of the associated endpoint
    - `event`: any term
    - `context`: a map of additional headers and properties relevant to "local bus". As of now,
    it only contains the application context under the key `:app_context`.

  Eventail events are:
    - `:waiting_for_config`: when configuration subscriptions are defined and the endpoint has joined the
      configuration bus
    - `:ready`: when the endpoint is connected and ready to sent/receive events on the EDA.

  Return value:
  `new_app_context` is the updated application context (not the whole context! so there is no `app_context` key in there).

  To implement a periodic health check (say, every minute), you could do something like
  ```
    @impl
    def handle_local(endpoint, :ready, context) do
      Eventail.publish_local(endpoint, :health, 60000)
      {:ok, context[:app_context]}
    end

    @impl
    def handle_local(endpoint, :health, context) do
      # do whatever you need to
      Logging.notice(endpoint, ...)
      {:ok, context[:app_context]}
    end
  ```

  See also `Eventail.publish_local/3`.
  """
  @callback handle_local(endpoint :: Endpoint.t(), event :: any(), context :: map()) ::
              {:ok, map()}

  @doc """
  Handle configuration events.

  This callback is called with the following positional arguments:
    - `endpoint`: the descriptor of the endpoint that received the event.
    - `config`: the config event name (routing key)
    - `payload`: the configuration payload as a term
    - `context`:  a map of additional headers and properties relevant to the EDA
    (in particular, the application context is under the `app_context` key).

  The callback must return either:
    - `{:ok, new_app_context}`: updates the application context (not the whole context! so there is no `app_context` key in there).
      If the endpoint is not ready yet (it has just started), tell it proceed to full operation (in particular, start to receive business events).
    - `{:wait, new_app_context}`: updates the application context too, but if the endpoint is not ready yet, tell it to wait for more configuration.
  """
  @callback handle_config(
              endpoint :: Endpoint.t(),
              config :: String.t(),
              payload :: any(),
              context :: map
            ) :: {:wait, map()} | {:ok, map()}
end
