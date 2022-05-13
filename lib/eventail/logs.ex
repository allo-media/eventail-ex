defmodule Eventail.Log do
  use GenServer
  @log_exchange "logs"
  @log_exchange_type :topic

  @spec start_link(atom()) :: GenServer.on_start()
  def start_link(channed_id) do
    GenServer.start_link(__MODULE__, channed_id)
  end

  @spec emergency(Eventail.Endpoint.t(), String.t(), String.t(), String.t(), keyword()) :: :ok
  def emergency(endpoint, short, full, conversation_id, addtional_fields \\ []) do
    log(endpoint, :EMERGENCY, 0, short, full, conversation_id, addtional_fields)
  end

  @spec alert(Eventail.Endpoint.t(), String.t(), String.t(), String.t(), keyword()) :: :ok
  def alert(endpoint, short, full, conversation_id, addtional_fields \\ []) do
    log(endpoint, :ALERT, 1, short, full, conversation_id, addtional_fields)
  end

  @spec critical(Eventail.Endpoint.t(), String.t(), String.t(), String.t(), keyword()) :: :ok
  def critical(endpoint, short, full, conversation_id, addtional_fields \\ []) do
    log(endpoint, :CRITICAL, 2, short, full, conversation_id, addtional_fields)
  end

  @spec error(Eventail.Endpoint.t(), String.t(), String.t(), String.t(), keyword()) :: :ok
  def error(endpoint, short, full, conversation_id, addtional_fields \\ []) do
    log(endpoint, :ERROR, 3, short, full, conversation_id, addtional_fields)
  end

  @spec warning(Eventail.Endpoint.t(), String.t(), String.t(), String.t(), keyword()) :: :ok
  def warning(endpoint, short, full, conversation_id, addtional_fields \\ []) do
    log(endpoint, :WARNING, 4, short, full, conversation_id, addtional_fields)
  end

  @spec notice(Eventail.Endpoint.t(), String.t(), String.t(), String.t(), keyword()) :: :ok
  def notice(endpoint, short, full, conversation_id, addtional_fields \\ []) do
    log(endpoint, :NOTICE, 5, short, full, conversation_id, addtional_fields)
  end

  @spec info(Eventail.Endpoint.t(), String.t(), String.t(), String.t(), keyword()) :: :ok
  def info(endpoint, short, full, conversation_id, addtional_fields \\ []) do
    log(endpoint, :INFO, 6, short, full, conversation_id, addtional_fields)
  end

  @spec debug(Eventail.Endpoint.t(), String.t(), String.t(), String.t(), keyword()) :: :ok
  def debug(endpoint, short, full, conversation_id, addtional_fields \\ []) do
    log(endpoint, :DEBUG, 7, short, full, conversation_id, addtional_fields)
  end

  defp log(
         endpoint,
         criticity_name,
         criticity_level,
         short,
         full,
         conversation_id,
         additional_fields
       ) do
    gelf =
      %{
        version: "1.1",
        short_message: short,
        full_message: full,
        level: criticity_level,
        host: "#{endpoint.logical_service}@#{endpoint.hostname}",
        timestamp: System.system_time(:millisecond) / 1000.0
      }
      |> Map.put("_levename", criticity_name)
      |> Map.put("_conversation_id", conversation_id)
      |> Map.put("_logical_service", endpoint.logical_service)
      |> Map.put("_worker_pid", inspect(self()))

    ex_gelf = Enum.reduce(additional_fields, gelf, fn {k, v}, m -> Map.put(m, "_" <> k, v) end)
    routing_key = "#{endpoint.logical_service}.#{criticity_name}"
    GenServer.cast(endpoint.log, {:publish, routing_key, ex_gelf})
  end

  # GenServer

  @impl GenServer
  def init(logical_service) do
    send(self(), :setup)
    {:ok, %{channel: nil, logical_service: logical_service}}
  end

  @impl GenServer
  def handle_cast({:publish, routing_key, gelf}, state) do
    unless state.channel == nil do
      AMQP.Basic.publish(state.channel, @log_exchange, routing_key, gelf)
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
    AMQP.Exchange.declare(channel, @log_exchange, @log_exchange_type, durable: true)
  end
end
