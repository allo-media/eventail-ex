defmodule Eventail do
  @moduledoc """
  Eventail for Elixir.

  This module contains the functions to configure and start an EDA endpoint service.
  """

  alias AMQP.Channel
  alias AMQP.Connection

  alias Eventail.Business
  alias Eventail.Config
  alias Eventail.Endpoint
  alias Eventail.Logging

  require Logger
  use GenServer

  # Configurations are special, the endpoint needs to be able to identify their origin,
  # so the the endpoint sets the configuration exchange name.
  @config_exchange "configurations"
  # in milliseconds
  @reconnect_interval 2_000
  # In seconds
  @heartbeat 5
  # in seconds
  @pending_check_interval 5
  # in milliseconds
  @pending_check_interval_ms @pending_check_interval * 1000

  #
  # Public API
  #

  @type config :: [option]

  @typedoc """
  Options values for Endpoint configuration.

  * `:exclusive_queues`: use private, non-durable queues (default to `false`)
  * `:prefetch_count`: tells RabbitMQ how many business events (events and commands) pending acknowledgment to push (default to `10`).
     If the concurrent message processing option is set, that value is the maximum number of messages
     handled concurrently.
  * `:concurrent_handlers`: if set, the incoming messages (except configuration messages) will be processed concurrently, each in its own task (defaults to false).
     Don't set this option if you must process the incoming messages in order!
     If message order is not important, concurrency is only useful if your handlers do a lot of work or I/O (
     for example, they query databases or REST APIs).
  * `:events`: list of the event names (routing keys) to subscribe to (defaults to `[]`)
  * `:commands`: list of the command/result names (routing keys) to subscribe to (defaults to `[]`)
  * `:configurations`: list of the configuration names (routing keys) to subscribe to (defaults to `[]`)
  * `:handlers`: A module implementing the `Eventail.Handlers` behavior (defaults to `nil`)
  * `app_context`: Application specific context that will be passed as is to the handlers (defaults to `nil`).
  """
  @type option ::
          {:exclusive_queues, boolean()}
          | {:prefetch_count, integer()}
          | {:concurrent_handlers, boolean()}
          | {:events, list(String.t())}
          | {:commands, list(String.t())}
          | {:configurations, list(String.t())}
          | {:handlers, module() | nil}
          | {:app_context, term()}

  @doc """
  Start an EDA endpoint.

  The endpoint automatically reconnects itself if the connection is lost.

  Parameters:

  * `hosts`: a list of rabbitmq hosts (from the same cluster)
  * `logical_service`: the logical service name to which this endpoint belongs
  * `name`: the very own name of the endpoint (process name)
  * `setup`: `t:config/0`


  The endpoint should be part of a supervision tree.
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
    GenServer.call(id, :endpoint)
  end

  # For tests
  @doc """
  Return the number of published messages awaiting broker confirm.
  """
  @spec get_pending_confirms(Endpoint.t()) :: non_neg_integer()
  def get_pending_confirms(endpoint) do
    GenServer.call(endpoint.pid, :pending_num)
  end

  @doc """
  Publisn an event local to the endpoint.
  Local events don't go on the EDA.

  - `endpoint` is the endpoint descriptor to which the local event is to be sent
  - `event` is any term; it will be passed to the `c:Eventail.Handlers.handle_local/3` handler
  - `delay` is the duration in millisecond to wait before delivering the message (it won't block this call)

  See also `c:Eventail.Handlers.handle_local/3`.
  """
  @spec publish_local(Endpoint.t(), any(), integer()) :: reference()
  def publish_local(endpoint, event, delay \\ 0) do
    Process.send_after(endpoint.pid, {:local, event}, delay)
  end

  #
  # GenServer
  #

  @impl GenServer
  def init(hosts: hosts, logical_service: logical_service, setup: setup) do
    # Ensure `terminate` is called on exit
    Process.flag(:trap_exit, true)
    endpoint = %Endpoint{logical_service: logical_service, pid: self(), hostname: get_hostname()}
    send(self(), :connect)
    Process.send_after(self(), :check_pending, @pending_check_interval_ms)

    {:ok,
     %{
       hosts: hosts,
       endpoint: endpoint,
       connection: nil,
       log_channel: nil,
       business_channel: nil,
       conf_channel: nil,
       pending_ack: :ets.new(logical_service, []),
       setup: setup
     }}
  end

  @impl GenServer
  def handle_cast({:log, exchange, routing_key, payload}, %{log_channel: log_channel} = state) do
    unless log_channel == nil do
      AMQP.Basic.publish(log_channel, exchange, routing_key, payload)
    end

    {:noreply, state}
  end

  @impl GenServer
  def handle_cast(
        {:emit_config, routing_key, payload, options},
        %{conf_channel: conf_channel} = state
      ) do
    unless conf_channel == nil do
      AMQP.Basic.publish(conf_channel, @config_exchange, routing_key, payload, options)
    end

    {:noreply, state}
  end

  @impl GenServer
  def handle_cast(
        {:publish, exchange, routing_key, payload, options} = msg,
        %{business_channel: channel, pending_ack: pending_ack} = state
      ) do
    msg_id =
      if channel != nil do
        msg_id = AMQP.Confirm.next_publish_seqno(channel)
        :ok = AMQP.Basic.publish(channel, exchange, routing_key, payload, options)
        msg_id
      else
        # Risk of collision is low
        System.monotonic_time(:millisecond)
      end

    :ets.insert(pending_ack, {msg_id, System.system_time(:second), msg})
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:endpoint, _, %{endpoint: endpoint} = state) do
    {:reply, endpoint, state}
  end

  @impl GenServer
  def handle_call(:pending_num, _, %{pending_ack: pending_ack} = state) do
    {:reply, :ets.info(pending_ack, :size), state}
  end

  @impl GenServer
  def handle_info({:basic_ack, seqno, false}, %{pending_ack: pending_ack} = state) do
    :ets.delete(pending_ack, seqno)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:basic_ack, seqno, true}, %{pending_ack: pending_ack} = state) do
    # select true from ets where msgid <= seqno
    :ets.select_delete(pending_ack, [{{:"$1", :_, :_}, [{:"=<", :"$1", seqno}], [true]}])
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:basic_nack, seqno, false}, %{pending_ack: pending_ack} = state) do
    msg = :ets.lookup_element(pending_ack, seqno, 3)
    :ets.delete(pending_ack, seqno)
    handle_cast(msg, state)
  end

  @impl GenServer
  def handle_info({:basic_nack, seqno, true}, %{pending_ack: pending_ack} = state) do
    # select msg from ets where msgid <= seqno
    messages = :ets.select(pending_ack, [{{:"$1", :_, :"$2"}, [{:"=<", :"$1", seqno}], [:"$2"]}])
    :ets.select_delete(pending_ack, [{{:"$1", :_, :"$2"}, [{:"=<", :"$1", seqno}], [true]}])
    resend_messages(messages)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(
        :check_pending,
        %{pending_ack: pending_ack, business_channel: business_channel} = state
      ) do
    unless business_channel == nil do
      limit = System.system_time(:second) - @pending_check_interval
      # select msg from ets where stamp <= limit
      messages =
        :ets.select(pending_ack, [{{:_, :"$1", :"$2"}, [{:"=<", :"$1", limit}], [:"$2"]}])

      :ets.select_delete(pending_ack, [{{:_, :"$1", :"$2"}, [{:"=<", :"$1", limit}], [true]}])
      resend_messages(messages)
    end

    Process.send_after(self(), :check_pending, @pending_check_interval_ms)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:local, event}, %{endpoint: endpoint, setup: setup} = state) do
    unless setup.handlers == nil do
      Task.start(setup.handlers, :handle_local, [
        endpoint,
        event,
        %{app_context: setup.app_context}
      ])
    end

    {:noreply, state}
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
        {:ok, log_channel} = Channel.open(conn)
        Logging.setup(log_channel)
        {:ok, conf_channel} = Channel.open(conn)
        Config.setup(conf_channel, @config_exchange, setup)

        business_channel =
          if Enum.empty?(setup.configurations) do
            {:ok, business_channel} = Channel.open(conn)
            Business.setup(business_channel, endpoint.logical_service, setup)
            send(self(), {:local, :ready})
            business_channel
          else
            send(self(), {:local, :waiting_for_config})
            nil
          end

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
    Logger.warning("Broker disconnected (#{inspect(reason)}). Reconnecting...")
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
  def handle_info(
        {:basic_cancel, %{consumer_tag: _consumer_tag}},
        %{business_channel: business_channel, endpoint: endpoint, setup: setup} = state
      ) do
    unless business_channel == nil do
      Business.setup(business_channel, endpoint.logical_service, setup)
    end

    {:noreply, state}
  end

  # Confirmation sent by the broker to the consumer process after a Basic.cancel
  @impl GenServer
  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(
        {:basic_deliver, body, %{exchange: @config_exchange} = context},
        %{
          endpoint: endpoint,
          business_channel: nil,
          conf_channel: conf_channel,
          connection: conn,
          setup: setup
        } = state
      ) do
    # must be a configuration message during startup
    case Config.handle_message(
           endpoint,
           conf_channel,
           body,
           context |> Map.put(:app_context, setup.app_context),
           setup.handlers
         ) do
      {:ok, app_context} ->
        {:ok, business_channel} = Channel.open(conn)
        Business.setup(business_channel, endpoint.logical_service, setup)
        send(self(), {:local, :ready})

        {:noreply,
         %{state | business_channel: business_channel, setup: %{setup | app_context: app_context}}}

      {:wait, app_context} ->
        {:noreply, %{state | setup: %{setup | app_context: app_context}}}
    end
  end

  @impl GenServer
  def handle_info(
        {:basic_deliver, body, %{exchange: @config_exchange} = context},
        %{endpoint: endpoint, conf_channel: conf_channel, setup: setup} = state
      ) do
    # Configuration message during normal operations
    {_, app_context} =
      Config.handle_message(
        endpoint,
        conf_channel,
        body,
        context |> Map.put(:app_context, setup.app_context),
        setup.handlers
      )

    {:noreply, %{state | setup: %{setup | app_context: app_context}}}
  end

  @impl GenServer
  def handle_info(
        {:basic_deliver, _,
         %{
           delivery_tag: tag,
           headers: :undefined,
           routing_key: routing_key
         }},
        %{endpoint: endpoint, business_channel: business_channel} = state
      ) do
    Logging.emergency(
      endpoint,
      "Missing headers",
      "No headers on #{routing_key}, dropping message",
      "none"
    )

    AMQP.Basic.reject(business_channel, tag, requeue: false)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(
        {:basic_deliver, body,
         %{
           headers: context_headers
         } = context},
        %{endpoint: endpoint, business_channel: business_channel, setup: setup} = state
      ) do
    eda_headers = headers(context_headers)
    context = Map.delete(context, :headers) |> Map.put(:app_context, setup.app_context)

    if setup.concurrent_handlers do
      {:ok, _} =
        Task.start(Business, :handle_message, [
          endpoint,
          business_channel,
          eda_headers,
          body,
          context,
          setup.handlers
        ])
    else
      Business.handle_message(
        endpoint,
        business_channel,
        eda_headers,
        body,
        context,
        setup.handlers
      )
    end

    {:noreply, state}
  end

  @impl GenServer
  def terminate(reason, %{
        endpoint: endpoint,
        connection: conn,
        business_channel: business_channel,
        log_channel: log_channel,
        conf_channel: conf_channel
      }) do
    Logger.info(
      "Endpoint #{endpoint.logical_service} shutting down for reason #{inspect(reason)}"
    )

    unless business_channel == nil do
      Channel.close(business_channel)
    end

    unless log_channel == nil do
      Channel.close(log_channel)
    end

    unless conf_channel == nil do
      Channel.close(conf_channel)
    end

    unless conn == nil do
      Connection.close(conn)
    end
  end

  #
  @spec headers(AMQP.arguments()) :: map()
  defp headers(amqp_headers) do
    Enum.reduce(amqp_headers, %{}, fn {k, _, v}, m ->
      Map.put(m, k, v)
    end)
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

  defp get_hostname do
    {:ok, hostname} = :inet.gethostname()
    List.to_string(hostname)
  end

  @spec setup(config()) :: map()
  defp setup(options) do
    defaults = %{
      exclusive_queues: false,
      prefetch_count: 10,
      concurrent_handlers: false,
      events: [],
      commands: [],
      configurations: [],
      handlers: nil,
      app_context: nil
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

  defp resend_messages([]) do
    :ok
  end

  defp resend_messages([msg | rest]) do
    GenServer.cast(self(), msg)
    resend_messages(rest)
  end
end
