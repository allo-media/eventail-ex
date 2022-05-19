defmodule Eventail.Logging do
  @moduledoc """
  EDA logging API
  """
  require Logger

  @exchange "logs"
  @exchange_type :topic

  #
  # Public API
  #

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

  #
  # Internal API
  #

  @doc false
  @spec setup(AMQP.Channel.t()) :: :ok
  def setup(channel) do
    :ok = AMQP.Exchange.declare(channel, @exchange, @exchange_type, durable: true)
  end

  @doc false
  @spec publish(AMQP.Channel.t(), String.t(), binary()) :: :ok | :error
  def publish(channel, routing_key, payload) do
    AMQP.Basic.publish(channel, @exchange, routing_key, payload)
  end

  # For tests
  def exchange do
    @exchange
  end

  #
  # Private functions
  #

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

    ex_gelf =
      Enum.reduce(additional_fields, gelf, fn {k, v}, m ->
        Map.put(m, "_" <> Atom.to_string(k), v)
      end)

    routing_key = "#{endpoint.logical_service}.#{criticity_name}"
    Logger.debug("Application logged on EDA: #{inspect(ex_gelf)}")

    # Privacy in Elixir does not extend to submodules (there is no such concept as submodule anyway).
    # So, we cast directly instead of using a function of `Eventail` that would have to be made public.
    GenServer.cast(endpoint.pid, {:log, routing_key, Jason.encode!(ex_gelf)})
  end
end
