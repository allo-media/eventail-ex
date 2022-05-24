defmodule Eventail.Business do
  @moduledoc """
  EDA business events and commands/results API
  """

  @event_exchange "events"
  @cmd_exchange "commands"
  @event_exchange_type :topic
  @cmd_exchange_type :topic

  alias Eventail.Endpoint
  alias Eventail.Logging

  #
  # Public API
  #

  @doc """
  Publish an event on the EDA.

  The parameters are:
  * `endpoint`: the descriptor of the endpoint through which the event is to be sent
  * `event`: the event name
  * `message`: the event message, that can be any serializable term, generaly a map or keyword list
  * `conversation_id`: The unique id of the "event thread" this event participates in. If this event
  reacts to a previous event, it should repeat the same `conversation_id`
  * `mandatory`: if this flag is set, the enpoint will crash if there are no subscriptions for the
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
        event,
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
      {:publish, @cmd_exchange, event, CBOR.encode(message), delivery_conf}
    )
  end

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
    case decode_body(body, content_type) do
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

    bind(channel, event_q, @event_exchange, events)
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

    bind(channel, cmd_q, @cmd_exchange, commands)
    {:ok, _} = AMQP.Basic.consume(channel, cmd_q)
  end

  defp bind(_, _, _, []) do
    :ok
  end

  defp bind(channel, queue, exchange, [key | rest]) do
    :ok = AMQP.Queue.bind(channel, queue, exchange, routing_key: key)
    bind(channel, queue, exchange, rest)
  end

  defp decode_body(body, "application/cbor") do
    case CBOR.decode(body) do
      {:ok, payload, _rest} -> {:ok, payload}
      err -> err
    end
  end

  defp decode_body(body, _) do
    Jason.decode(body)
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
