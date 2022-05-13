defmodule Eventail.Business do
  @moduledoc """
  EDA business events and commands/results.

  Depends on configured channel `:business`.
  """

  use GenServer
  @channel :business
  @event_exchange "events"
  @cmd_exchange "commands"
  @event_exchange_type :topic
  @cmd_exchange_type :topic

  # GenServer

  @impl GenServer
  def init(logical_service) do
    send(self(), :setup)
    {:ok, %{channel: nil, logical_service: logical_service}}
  end

  @impl GenServer
  def handle_cast({:publish, routing_key, gelf}, state) do
    unless state.channel == nil do
      AMQP.Basic.publish(state.channel, @event_exchange, routing_key, gelf)
      # TODO: else
    end

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:setup, %{logical_service: logical_service} = state) do
    case setup(logical_service) do
      {:ok, chan} ->
        {:noreply, Map.put(state, :channel, chan)}

      _ ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _, :process, pid, _reason}, %{channel: %{pid: pid}} = state) do
    send(self(), :setup)
    {:noreply, Map.put(state, :channel, nil)}
  end

  @spec setup(atom()) :: {:ok, AMQP.Channel.t()} | {:error, :retrying}
  defp setup(logical_service) do
    case Eventail.open_channel(logical_service) do
      {:ok, chan} ->
        Process.monitor(chan.pid)
        :ok = setup_topology(chan)
        {:ok, chan}

      _error ->
        Process.send_after(self(), :setup, 1000)
        {:error, :retrying}
    end
  end

  defp setup_topology(channel) do
    AMQP.Exchange.declare(channel, @event_exchange, @event_exchange_type, durable: true)
  end
end
