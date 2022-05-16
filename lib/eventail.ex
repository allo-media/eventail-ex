defmodule Eventail do
  @moduledoc """
  Eventail for Elixir.
  """

  alias AMQP.Channel
  alias AMQP.Connection
  alias Eventail.Business
  alias Eventail.Endpoint
  alias Eventail.Logging
  require Logger
  use GenServer

  @reconnect_interval 2_000
  @heartbeat 5

  @doc """
  Start an AMQP Connection supervisor.
  * `hosts`: a list of rabbitmq hosts (from the same cluster)
  * `name`: the name of this connection
  * `exclusive_queues`: use private, non-durable queues?
  * `events`: list of the event names (routing keys) to subscribe to
  * `cammands`: list of the command/result names (routing keys) to subscribe to
  """
  @spec start_link(
          hosts: list(String.t()),
          name: atom(),
          exclusive_queues: boolean(),
          events: list(String.t()),
          commands: list(String.t())
        ) ::
          {:ok, pid} | {:error, term} | :ignore
  def start_link(
        hosts: hosts,
        name: name,
        exclusive_queues: exclusive_queues,
        events: events,
        commands: commands
      ) do
    endpoint = %Endpoint{logical_service: name, pid: nil, hostname: get_hostname()}

    GenServer.start_link(
      __MODULE__,
      %{
        hosts: hosts,
        endpoint: endpoint,
        connection: nil,
        log_channel: nil,
        business_channel: nil,
        conf_channel: nil,
        setup: [exclusive_queues: exclusive_queues, events: events, commands: commands]
      },
      name: name
    )
  end

  @spec get_endpoint(pid() | atom()) :: Endpoint.t()
  def get_endpoint(id) do
    GenServer.call(id, :get)
  end

  #
  # GenServer
  #

  @impl GenServer
  def init(%{endpoint: endpoint} = state) do
    send(self(), :connect)
    resolved_endpoint = %{endpoint | pid: self()}
    {:ok, %{state | endpoint: resolved_endpoint}}
  end

  @impl GenServer
  def handle_cast({:log, routing_key, payload}, %{log_channel: log_channel} = state) do
    unless log_channel == nil do
      Logging.publish(log_channel, routing_key, payload)
      # TODO: else
    end

    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:get, _, %{endpoint: endpoint} = state) do
    {:reply, endpoint, state}
  end

  @impl GenServer
  def handle_info(
        :connect,
        %{hosts: hosts, endpoint: endpoint, setup: setup} = state
      ) do
    case connect(hosts) do
      {:ok, conn} ->
        # Get notifications when the connection goes down
        Logger.info("Connected to broker!")
        Process.monitor(conn.pid)
        {:ok, business_channel} = Channel.open(conn)
        Business.setup(business_channel, endpoint.logical_service, setup)
        {:ok, log_channel} = Channel.open(conn)
        Logging.setup(log_channel)
        {:ok, conf_channel} = Channel.open(conn)

        {:noreply,
         %{
           state
           | connection: conn,
             business_channel: business_channel,
             log_channel: log_channel,
             conf_channel: conf_channel
         }}

      {:error, reason} ->
        Logger.error(
          "Failed to connect to a broker for reason #{inspect(reason)}. Reconnecting later..."
        )

        # Retry later
        Process.send_after(self(), :connect, @reconnect_interval)
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _, :process, _pid, reason}, state) do
    Logger.error("Broker disconnected (#{inspect(reason)}). Reconnecting...")
    send(self(), :connect)

    {:noreply,
     %{state | connection: nil, log_channel: nil, business_channel: nil, conf_channel: nil}}
  end

  defp connect([]) do
    {:error, "All known brokers are unavailable"}
  end

  defp connect([host | hosts]) do
    case Connection.open(host, heartbeat: @heartbeat) do
      {:ok, conn} ->
        {:ok, conn}

      {:error, reason} ->
        Logger.error("Connection to #{host} failed for reason #{inspect(reason)}")
        connect(hosts)
    end
  end

  def get_hostname() do
    {:ok, hostname} = :inet.gethostname()
    List.to_string(hostname)
  end
end
