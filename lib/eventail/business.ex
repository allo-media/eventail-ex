defmodule Eventail.Business do
  @moduledoc """
  EDA business events and commands/results API
  """

  require Logger

  @event_exchange "events"
  @cmd_exchange "commands"
  @event_exchange_type :topic
  @cmd_exchange_type :topic

  alias Eventail.Endpoint
  alias Eventail.Logging
  alias Eventail.Utils

  #
  # Public API
  #

  @doc """
  Publish an event on the EDA.

  The parameters are:
    * `endpoint`: the descriptor of the endpoint through which the event is to be sent
    * `event`: the event name
    * `message`: the event message, that can be any serializable term, generally a map or keyword list
    * `conversation_id`: The unique id of the "message thread" this event participates in. If this event
    reacts to a previous event, it should repeat the same `conversation_id`
    * `mandatory`: if this flag is set, the endpoint will crash if there are no subscriptions for the
    sent event (for debug purposes)
  """
  @spec publish_event(Endpoint.t(), String.t() | atom(), any(), String.t(), boolean()) :: :ok
  def publish_event(endpoint, event, message, conversation_id, mandatory \\ false) do
    headers = [{"conversation_id", :longstr, conversation_id}]

    delivery_conf = [
      mandatory: mandatory,
      headers: headers,
      persistent: true,
      content_type: "application/cbor"
    ]

    GenServer.cast(
      endpoint.pid,
      {:publish, @event_exchange, event, CBOR.encode(message), delivery_conf}
    )
  end

  @doc """
  Send a command to a service on the EDA.

  The parameters are:
    * `endpoint`: the descriptor of the endpoint through which the command is to be sent
    * `command`: the command name
    * `message`: the command message, that can be any serializable term, generally a map or keyword list
    * `conversation_id`: The unique id of the "message thread" this command participates in. If this command
    reacts to a previous command, result or event, it should repeat the same `conversation_id`
    * `reply_to`: the return address where to send the response (result)
    * `correlation_id`: whatever application dependent reference string that should be attached to the result.
      As the response (result) is received asynchronously later, the `correlation_id` is used to reconcile it
      with the original command
    * `mandatory`: if this flag is set, the endpoint will crash if there are no subscriptions for the
    sent command (for debug purposes)

  """
  @spec send_command(
          Endpoint.t(),
          String.t() | atom(),
          any(),
          String.t(),
          String.t() | atom(),
          String.t(),
          boolean()
        ) :: :ok
  def send_command(
        endpoint,
        command,
        message,
        conversation_id,
        reply_to,
        correlation_id,
        mandatory \\ false
      ) do
    headers = [{"conversation_id", :longstr, conversation_id}]

    delivery_conf = [
      mandatory: mandatory,
      headers: headers,
      persistent: true,
      content_type: "application/cbor",
      reply_to: reply_to,
      correlation_id: correlation_id
    ]

    GenServer.cast(
      endpoint.pid,
      {:publish, @cmd_exchange, command, CBOR.encode(message), delivery_conf}
    )
  end

  @doc """
  Send the successful result of a command to the requesting service.

  The parameters are:
    * `endpoint`: the descriptor of the endpoint through which the result is to be sent
    * `dest`: should be equal to the `reply_to` of the command this result responds to
    * `message`: the result message, that can be any serializable term, generally a map or keyword list
    * `conversation_id`: The unique id of the "message thread" this result participates in.
       It should be the same `conversation_id` as the command this result responds to
    * `correlation_id`: the `correlation_id` of the original command, unchanged
    * `mandatory`: if this flag is set, the endpoint will crash if there are no subscriptions for the
    sent result (for debug purposes)

  """
  @spec return_success(
          Endpoint.t(),
          String.t() | atom(),
          any(),
          String.t(),
          String.t(),
          boolean()
        ) :: :ok
  def return_success(endpoint, dest, message, conversation_id, correlation_id, mandatory \\ false) do
    return_result(endpoint, "success", dest, message, conversation_id, correlation_id, mandatory)
  end

  @doc """
  Send the error result of a command to the requesting service.

  The parameters are:
    * `endpoint`: the descriptor of the endpoint through which the result is to be sent
    * `dest`: should be equal to the `reply_to` of the command this result responds to
    * `message`: the result message, that can be any serializable term, generally a map or keyword list
    * `conversation_id`: The unique id of the "message thread" this result participates in.
       It should be the same `conversation_id` as the command this result responds to
    * `correlation_id`: the `correlation_id` of the original command, unchanged
    * `mandatory`: if this flag is set, the endpoint will crash if there are no subscriptions for the
    sent result (for debug purposes)

  """
  @spec return_error(Endpoint.t(), String.t() | atom(), any(), String.t(), String.t(), boolean()) ::
          :ok
  def return_error(endpoint, dest, message, conversation_id, correlation_id, mandatory \\ false) do
    return_result(endpoint, "error", dest, message, conversation_id, correlation_id, mandatory)
  end

  #
  # Internal API
  #

  @doc false
  @spec setup(AMQP.Channel.t(), atom(), map()) :: :ok
  def setup(channel, logical_service, %{
        exclusive_queues: exclusive_queues,
        prefetch_count: prefetch_count,
        events: events,
        commands: commands
      }) do
    :ok = AMQP.Confirm.select(channel)
    :ok = AMQP.Confirm.register_handler(channel, self())
    :ok = AMQP.Basic.qos(channel, prefetch_count: prefetch_count)
    :ok = AMQP.Exchange.declare(channel, @event_exchange, @event_exchange_type, durable: true)
    :ok = AMQP.Exchange.declare(channel, @cmd_exchange, @cmd_exchange_type, durable: true)
    setup_events(channel, logical_service, exclusive_queues, events)
    setup_commands(channel, logical_service, exclusive_queues, commands)
  end

  @doc false
  @spec handle_message(Eventail.Endpoint.t(), AMQP.Channel.t(), map(), binary(), map(), module()) ::
          :ok
  def handle_message(
        endpoint,
        channel,
        %{"conversation_id" => conversation_id} = headers,
        body,
        %{
          delivery_tag: tag,
          routing_key: routing_key,
          content_type: content_type,
          exchange: exchange,
          redelivered: redelivered
        } = context,
        handlers
      ) do
    case Utils.decode_body(body, content_type) do
      {:error, msg} ->
        Logging.emergency(
          endpoint,
          "Unable to decode payload for #{routing_key}",
          inspect(msg),
          conversation_id
        )

        AMQP.Basic.ack(channel, tag)

      {:ok, payload} ->
        if exchange == @event_exchange do
          handle_event(endpoint, channel, headers, payload, context, handlers)
        else
          handle_command(endpoint, channel, headers, payload, context, handlers)
        end
    end
  rescue
    # Requeue unless it's a redelivered message.
    # This means we will retry consuming a message once in case of exception
    # before we give up and have it moved to the error queue
    exception ->
      if redelivered do
        Logging.emergency(
          endpoint,
          "Giving up on #{routing_key}",
          inspect(exception),
          conversation_id
        )
      else
        Logging.alert(
          endpoint,
          "Unhandled error while processing message #{routing_key}",
          inspect(exception),
          conversation_id
        )
      end

      :ok = AMQP.Basic.reject(channel, tag, requeue: not redelivered)
  catch
    :exit, reason ->
      # Task crash is exceptional and independent from the task, so we always retry
      Logging.alert(
        endpoint,
        "Handler process unexpectedly crashed while processing #{routing_key}",
        inspect(reason),
        conversation_id
      )

      :ok = AMQP.Basic.reject(channel, tag, requeue: true)
  end

  @doc false
  def handle_message(
        endpoint,
        channel,
        _,
        _,
        %{
          delivery_tag: tag,
          routing_key: routing_key
        },
        _
      ) do
    Logging.emergency(
      endpoint,
      "Missing conversation_id",
      "No conversation_id on #{routing_key}, dropping message",
      "none"
    )

    AMQP.Basic.ack(channel, tag)
  end

  # For tests
  @doc false
  def event_exchange do
    @event_exchange
  end

  # For tests
  @doc false
  def command_exchange do
    @cmd_exchange
  end

  defp setup_events(_, _, _, []) do
    :ok
  end

  defp setup_events(channel, logical_service, exclusive_queues, events) do
    evt_qname =
      if exclusive_queues do
        ""
      else
        "#{logical_service}.events"
      end

    {:ok, %{queue: event_q}} =
      AMQP.Queue.declare(channel, evt_qname,
        exclusive: exclusive_queues,
        durable: not exclusive_queues
      )

    Utils.bind(channel, event_q, @event_exchange, events)
    Logger.info("Service #{logical_service} subscribed to business events #{inspect(events)}")
    {:ok, _} = AMQP.Basic.consume(channel, event_q)
  end

  defp setup_commands(_, _, _, []) do
    :ok
  end

  defp setup_commands(channel, logical_service, exclusive_queues, commands) do
    cmd_qname =
      if exclusive_queues do
        ""
      else
        "#{logical_service}.commands"
      end

    {:ok, %{queue: cmd_q}} =
      AMQP.Queue.declare(channel, cmd_qname,
        exclusive: exclusive_queues,
        durable: not exclusive_queues
      )

    Utils.bind(channel, cmd_q, @cmd_exchange, commands)
    Logger.info("Service #{logical_service} subscribed to commands/results #{inspect(commands)}")
    {:ok, _} = AMQP.Basic.consume(channel, cmd_q)
  end

  @spec handle_event(Eventail.Endpoint.t(), AMQP.Channel.t(), map(), map(), map(), module()) ::
          :ok
  defp handle_event(
         endpoint,
         channel,
         headers,
         payload,
         %{
           routing_key: routing_key,
           delivery_tag: tag
         } = context,
         handlers
       ) do
    {conversation_id, app_headers} = Map.pop(headers, "conversation_id")

    handlers.handle_event(
      endpoint,
      routing_key,
      payload,
      conversation_id,
      Map.merge(context, app_headers)
    )

    AMQP.Basic.ack(channel, tag)
  end

  @spec handle_command(Eventail.Endpoint.t(), AMQP.Channel.t(), map(), map(), map(), module()) ::
          :ok
  defp handle_command(
         endpoint,
         channel,
         headers,
         payload,
         %{
           delivery_tag: tag,
           routing_key: routing_key,
           correlation_id: correlation_id,
           reply_to: reply_to
         } = context,
         handlers
       ) do
    {conversation_id, h} = Map.pop(headers, "conversation_id")
    {status, h} = Map.pop(h, "status", nil)
    app_headers = Map.merge(context, h)

    cond do
      reply_to != :undefined ->
        handlers.handle_command(
          endpoint,
          routing_key,
          payload,
          conversation_id,
          reply_to,
          correlation_id,
          app_headers
        )

        AMQP.Basic.ack(channel, tag)

      status ->
        handlers.handle_result(
          endpoint,
          routing_key,
          payload,
          conversation_id,
          status,
          correlation_id,
          app_headers
        )

        AMQP.Basic.ack(channel, tag)

      true ->
        Logging.emergency(
          endpoint,
          "Invalid enveloppe for command/result #{routing_key}",
          "Missing reply_to and status",
          conversation_id
        )

        AMQP.Basic.ack(channel, tag)
    end
  end

  defp return_result(endpoint, status, dest, message, conversation_id, correlation_id, mandatory) do
    headers = [{"conversation_id", :longstr, conversation_id}, {"status", :longstr, status}]

    delivery_conf = [
      mandatory: mandatory,
      headers: headers,
      persistent: true,
      content_type: "application/cbor",
      correlation_id: correlation_id
    ]

    GenServer.cast(
      endpoint.pid,
      {:publish, @cmd_exchange, dest, CBOR.encode(message), delivery_conf}
    )
  end
end
