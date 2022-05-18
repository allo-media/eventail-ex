defmodule Eventail.Business do
  @moduledoc """
  EDA business events and commands/results API
  """

  @event_exchange "events"
  @cmd_exchange "commands"
  @event_exchange_type :topic
  @cmd_exchange_type :topic

  #
  # Internal API
  #

  @spec setup(AMQP.Channel.t(), atom(), map()) :: :ok
  def setup(channel, logical_service, %{
        exclusive_queues: exclusive_queues,
        prefetch_count: prefetch_count,
        events: events,
        commands: commands
      }) do
    :ok = AMQP.Confirm.select(channel)
    :ok = AMQP.Basic.qos(channel, prefetch_count: prefetch_count)
    :ok = AMQP.Exchange.declare(channel, @event_exchange, @event_exchange_type, durable: true)
    :ok = AMQP.Exchange.declare(channel, @cmd_exchange, @cmd_exchange_type, durable: true)
    setup_events(channel, logical_service, exclusive_queues, events)
    setup_commands(channel, logical_service, exclusive_queues, commands)
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
        durable: !exclusive_queues
      )

    bind(channel, event_q, @event_exchange, events)
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
        durable: !exclusive_queues
      )

    bind(channel, cmd_q, @cmd_exchange, commands)
  end

  defp bind(_, _, _, []) do
    :ok
  end

  defp bind(channel, queue, exchange, [key | rest]) do
    :ok = AMQP.Queue.bind(channel, queue, exchange, routing_key: key)
    bind(channel, queue, exchange, rest)
  end
end
