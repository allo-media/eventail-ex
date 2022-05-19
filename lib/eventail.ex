defmodule Eventail do
  @moduledoc """
  Eventail for Elixir.

  This module contains the functions to configure and start an EDA endpoint service.
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

  #
  # Public API
  #

  @type config :: [option]

  @typedoc """
  Options values for Enpoint configuration.

  * `:exclusive_queues`: use private, non-durable queues (default to `false`)
  * `:prefetch_count`: tells RabbitMQ how many events pending acknowledgment to push (default to `10`).
     If the concurrent message processing option is set, that value is the maximum number of messages
     handled concurrently.
  * `:concurrent_processing`: if set, the incoming messages will be processed concurrently (defaults to false).
     Don't set this option if you must process the incoming messages in order!
     If message order is not important, concurrency is only useful if your handlers do a lot of work or I/O (
     for example, they query databases or REST APIs).
  * `:events`: list of the event names (routing keys) to subscribe to (defaults to `[]`)
  * `:commands`: list of the command/result names (routing keys) to subscribe to (defaults to `[]`)
  * `:handlers`: A module implementing the `Eventail.Handlers` behavior (defaults to nil)
  """
  @type option ::
          {:exclusive_queues, boolean()}
          | {:prefetch_count, integer()}
          | {:concurrent_processing, boolean()}
          | {:events, list(String.t())}
          | {:commands, list(String.t())}
          | {:handlers, module() | nil}

  @doc """
  Start an AMQP Connection supervisor.
  * `hosts`: a list of rabbitmq hosts (from the same cluster)
  * `logical_service`: the logical service name to which this endpoint belongs.
  * `setup`: `t:config/0`

  """
  @spec start_link(
          hosts: list(String.t()),
          logical_service: atom(),
          name: atom(),
          setup: config()
        ) ::
          {:ok, pid} | {:error, term} | :ignore
  def start_link(
        hosts: hosts,
        logical_service: logical_service,
        name: name,
        setup: config
      ) do
    GenServer.start_link(
      __MODULE__,
      [
        hosts: hosts,
        logical_service: logical_service,
        setup: setup(config)
      ],
      name: name
    )
  end

  @doc """
  Retrieve the endpoint descriptor for the given endpoint name or pid.
  The endpoint descriptor is required by all the API functions.
  """
  @spec get_endpoint(pid() | atom()) :: Endpoint.t()
  def get_endpoint(id) do
    GenServer.call(id, :get)
  end

  #
  # GenServer
  #

  @impl GenServer
  def init(hosts: hosts, logical_service: logical_service, setup: setup) do
    endpoint = %Endpoint{logical_service: logical_service, pid: self(), hostname: get_hostname()}
    send(self(), :connect)

    {:ok,
     %{
       hosts: hosts,
       endpoint: endpoint,
       connection: nil,
       log_channel: nil,
       business_channel: nil,
       conf_channel: nil,
       setup: setup
     }}
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

  # Confirmation sent by the broker after registering this process as a consumer
  @impl GenServer
  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  @impl GenServer
  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, state) do
    {:stop, :normal, state}
  end

  # Confirmation sent by the broker to the consumer process after a Basic.cancel
  @impl GenServer
  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(
        {:basic_deliver, payload, %{delivery_tag: tag, redelivered: redelivered}} = meta,
        %{endpoint: endpoint, setup: setup} = state
      ) do
    Logger.info(inspect(meta))
    {:noreply, state}
  end

  #

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

  defp get_hostname do
    {:ok, hostname} = :inet.gethostname()
    List.to_string(hostname)
  end

  @spec setup(config()) :: map()
  defp setup(options) do
    defaults = %{
      exclusive_queues: false,
      prefetch_count: 10,
      concurrent_processing: false,
      events: [],
      commands: [],
      handlers: nil
    }

    config =
      Enum.reduce(options, defaults, fn {k, v}, m ->
        Map.put(m, k, v)
      end)

    # Handlers must exist if subscriptions are not empty
    (Enum.empty?(config.events) and Enum.empty?(config.commands)) or config.handlers != nil or
      raise "Missing handlers for subscriptions!"

    config
  end
end
