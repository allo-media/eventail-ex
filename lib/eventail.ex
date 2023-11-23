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
  * `:events`: list of the event names (routing keys) to subscribe to (defaults to `[]`)
  * `:commands`: list of the command/result names (routing keys) to subscribe to (defaults to `[]`)
  * `:configurations`: list of the configuration names (routing keys) to subscribe to (defaults to `[]`)
  * `:handlers`: A module implementing the `Eventail.Handlers` behavior (defaults to `nil`)
  * `app_context`: Application specific context that will be passed as is to the handlers (defaults to `nil`).
  """
  @type option ::
          {:exclusive_queues, boolean()}
          | {:prefetch_count, integer()}
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
  Publish an event local to the endpoint.
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
       main_channel: nil,
       events_consumer_tag: nil,
       cmd_consumer_tag: nil,
       config_consumer_tag: nil,
       # {message_tag, timestamp, message}
       pending_publishes: :ets.new(logical_service, [:ordered_set]),
       last_published_tag: 0,
       # {delivery_tag, last_published_tag}
       pending_consumer_acks: [],
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
        state
      ) do
    handle_cast({:publish, @config_exchange, routing_key, payload, options}, state)
  end

  @impl GenServer
  def handle_cast(
        {:publish, exchange, routing_key, payload, options} = msg,
        %{
          main_channel: channel,
          pending_publishes: pending_publishes,
          last_published_tag: last_published_tag
        } = state
      ) do
    msg_id =
      if channel != nil do
        msg_id = AMQP.Confirm.next_publish_seqno(channel)
        :ok = AMQP.Basic.publish(channel, exchange, routing_key, payload, options)
        msg_id
      else
        # safe as we replay all on reconnect
        last_published_tag + 1
      end

    :ets.insert(pending_publishes, {msg_id, System.system_time(:second), msg})
    {:noreply, %{state | last_published_tag: msg_id}}
  end

  @impl GenServer
  def handle_cast({:manual_ack, delivery_tag, options}, %{main_channel: channel} = state) do
    AMQP.Basic.ack(channel, delivery_tag, options)
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:manual_nack, delivery_tag, options}, %{main_channel: channel} = state) do
    AMQP.Basic.nack(channel, delivery_tag, options)
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:endpoint, _, %{endpoint: endpoint} = state) do
    {:reply, endpoint, state}
  end

  @impl GenServer
  def handle_call(:pending_num, _, %{pending_publishes: pending_publishes} = state) do
    {:reply, :ets.info(pending_publishes, :size), state}
  end

  @impl GenServer
  def handle_info(
        {:basic_ack, seqno, false},
        %{
          pending_publishes: pending_publishes,
          pending_consumer_acks: pending_consumer_acks,
          main_channel: channel
        } = state
      ) do
    :ets.delete(pending_publishes, seqno)

    {:noreply,
     %{
       state
       | pending_consumer_acks: ack_completed(pending_consumer_acks, pending_publishes, channel)
     }}
  end

  @impl GenServer
  def handle_info(
        {:basic_ack, seqno, true},
        %{
          pending_publishes: pending_publishes,
          pending_consumer_acks: pending_consumer_acks,
          main_channel: channel
        } = state
      ) do
    # select true from ets where msgid <= seqno
    :ets.select_delete(pending_publishes, [{{:"$1", :_, :_}, [{:"=<", :"$1", seqno}], [true]}])

    {:noreply,
     %{
       state
       | pending_consumer_acks: ack_completed(pending_consumer_acks, pending_publishes, channel)
     }}
  end

  @impl GenServer
  def handle_info({:basic_nack, seqno, false}, %{pending_publishes: pending_publishes} = state) do
    msg = :ets.lookup_element(pending_publishes, seqno, 3)
    :ets.delete(pending_publishes, seqno)

    {:noreply,
     %{last_published_tag: last_published_tag, pending_consumer_acks: pending_consumer_acks} =
       state} = handle_cast(msg, state)

    {:noreply,
     %{
       state
       | pending_consumer_acks: reschedule_acks(pending_consumer_acks, seqno, last_published_tag)
     }}
  end

  @impl GenServer
  def handle_info({:basic_nack, seqno, true}, %{pending_publishes: pending_publishes} = state) do
    # select msg from ets where msgid <= seqno
    messages =
      :ets.select(pending_publishes, [{{:"$1", :_, :"$2"}, [{:"=<", :"$1", seqno}], [:"$2"]}])

    keys =
      :ets.select(pending_publishes, [{{:"$1", :_, :"$2"}, [{:"=<", :"$1", seqno}], [:"$1"]}])

    :ets.select_delete(pending_publishes, [{{:"$1", :_, :"$2"}, [{:"=<", :"$1", seqno}], [true]}])
    lower_seqno = List.first(keys)

    %{last_published_tag: last_published_tag, pending_consumer_acks: pending_consumer_acks} =
      new_state = resend_messages(messages, state)

    {:noreply,
     %{
       new_state
       | pending_consumer_acks:
           reschedule_acks(pending_consumer_acks, lower_seqno, last_published_tag)
     }}
  end

  @impl GenServer
  def handle_info(
        :check_pending,
        state
      ) do
    last_state = flush_pending(state, @pending_check_interval)

    Process.send_after(self(), :check_pending, @pending_check_interval_ms)
    {:noreply, last_state}
  end

  @impl GenServer
  def handle_info({:local, event}, %{endpoint: endpoint, setup: setup} = state) do
    if is_nil(setup.handlers) do
      {:noreply, state}
    else
      {:ok, app_context} =
        setup.handlers.handle_local(
          endpoint,
          event,
          %{app_context: setup.app_context}
        )

      {:noreply, %{state | setup: %{setup | app_context: app_context}}}
    end
  end

  @impl GenServer
  def handle_info(
        :connect,
        %{hosts: hosts, endpoint: endpoint, setup: setup} = state
      ) do
    case connect(hosts) do
      {:ok, conn} ->
        Logger.info("Connected to broker!")
        # Get notifications when the connection goes down
        Process.monitor(conn.pid)
        {:ok, log_channel} = Channel.open(conn)
        Logging.setup(log_channel)
        {:ok, main_channel} = Channel.open(conn)
        Config.setup(main_channel, @config_exchange)
        Business.setup(main_channel, setup)
        state = flush_pending(state, 0)

        if Enum.empty?(setup.configurations) do
          new_state = %{
            state
            | connection: conn,
              main_channel: main_channel,
              log_channel: log_channel,
              events_consumer_tag:
                Business.consume_events(main_channel, endpoint.logical_service, setup),
              cmd_consumer_tag:
                Business.consume_commands(main_channel, endpoint.logical_service, setup)
          }

          send(self(), {:local, :ready})
          {:noreply, new_state}
        else
          new_state = %{
            state
            | connection: conn,
              main_channel: main_channel,
              log_channel: log_channel,
              config_consumer_tag: Config.consume(main_channel, @config_exchange, setup)
          }

          send(self(), {:local, :waiting_for_config})
          {:noreply, new_state}
        end

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
     %{
       state
       | connection: nil,
         log_channel: nil,
         main_channel: nil,
         events_consumer_tag: nil,
         cmd_consumer_tag: nil,
         config_consumer_tag: nil,
         last_published_tag: nil,
         pending_consumer_acks: []
     }}
  end

  # Confirmation sent by the broker after registering this process as a consumer
  @impl GenServer
  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  @impl GenServer
  def handle_info(
        {:basic_cancel, %{consumer_tag: consumer_tag}},
        %{
          main_channel: main_channel,
          endpoint: endpoint,
          setup: setup,
          events_consumer_tag: events_tag,
          cmd_consumer_tag: cmd_tag,
          config_consumer_tag: conf_tag
        } = state
      ) do
    unless main_channel == nil do
      case consumer_tag do
        ^events_tag ->
          {:noreply,
           %{
             state
             | events_consumer_tag:
                 Business.consume_events(main_channel, endpoint.logical_service, setup)
           }}

        ^cmd_tag ->
          {:noreply,
           %{
             state
             | cmd_consumer_tag:
                 Business.consume_commands(main_channel, endpoint.logical_service, setup)
           }}

        ^conf_tag ->
          {:noreply,
           %{state | config_consumer_tag: Config.consume(main_channel, @config_exchange, setup)}}
      end
    end
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
          main_channel: main_channel,
          events_consumer_tag: events_consumer,
          cmd_consumer_tag: commands_consumer,
          setup: setup
        } = state
      ) do
    # must be a configuration message during startup
    case Config.handle_message(
           endpoint,
           main_channel,
           body,
           context |> Map.put(:app_context, setup.app_context),
           setup.handlers
         ) do
      {:ok, app_context} when events_consumer == nil and commands_consumer == nil ->
        ect = Business.consume_events(main_channel, endpoint.logical_service, setup)
        cct = Business.consume_commands(main_channel, endpoint.logical_service, setup)
        send(self(), {:local, :ready})

        {:noreply,
         %{
           state
           | events_consumer_tag: ect,
             cmd_consumer_tag: cct,
             setup: %{setup | app_context: app_context}
         }}

      {:ok, app_context} ->
        {:noreply, %{state | setup: %{setup | app_context: app_context}}}

      {:wait, app_context} ->
        {:noreply, %{state | setup: %{setup | app_context: app_context}}}
    end
  end

  @impl GenServer
  def handle_info(
        {:basic_deliver, _,
         %{
           delivery_tag: tag,
           headers: :undefined,
           routing_key: routing_key
         }},
        %{endpoint: endpoint, main_channel: main_channel} = state
      ) do
    Logging.emergency(
      endpoint,
      "Missing headers",
      "No headers on #{routing_key}, dropping message",
      "none"
    )

    AMQP.Basic.reject(main_channel, tag, requeue: false)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(
        {:basic_deliver, body,
         %{
           headers: context_headers
         } = context},
        %{
          endpoint: endpoint,
          main_channel: main_channel,
          setup: setup,
          pending_consumer_acks: pending_consumer_acks,
          last_published_tag: last_published_tag
        } = state
      ) do
    eda_headers = headers(context_headers)
    context = Map.delete(context, :headers) |> Map.put(:app_context, setup.app_context)

    {:ok, app_context, tag} =
      Business.handle_message(
        endpoint,
        main_channel,
        eda_headers,
        body,
        context,
        setup.handlers
      )

    case {tag, pending_consumer_acks} do
      {nil, _} ->
        {:noreply, %{state | setup: %{setup | app_context: app_context}}}

      {val, []} ->
        AMQP.Basic.ack(main_channel, val)
        {:noreply, %{state | setup: %{setup | app_context: app_context}}}

      {val, acks} ->
        {:noreply,
         %{
           state
           | setup: %{setup | app_context: app_context},
             pending_consumer_acks: [{val, last_published_tag} | acks]
         }}
    end
  end

  @impl GenServer
  def terminate(reason, %{
        endpoint: endpoint,
        connection: conn,
        main_channel: main_channel,
        log_channel: log_channel
      }) do
    Logger.info(
      "Endpoint #{endpoint.logical_service} shutting down for reason #{inspect(reason)}"
    )

    unless main_channel == nil do
      Channel.close(main_channel)
    end

    unless log_channel == nil do
      Channel.close(log_channel)
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
        domain = String.split(host, "@") |> List.last()
        Logger.error("Connection to #{domain} failed for reason #{inspect(reason)}")
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

  defp resend_messages([], state) do
    state
  end

  defp resend_messages([msg | rest], state) do
    {:noreply, new_state} = handle_cast(msg, state)
    resend_messages(rest, new_state)
  end

  defp ack_completed(pending_consumer_acks, pending_publishes, channel) do
    case :ets.first(pending_publishes) do
      :"$end_of_table" ->
        Enum.each(pending_consumer_acks, fn {tag, _} -> AMQP.Basic.ack(channel, tag) end)
        []

      bound ->
        {still_pending, ready} = split_at(pending_consumer_acks, bound)
        Enum.each(ready, fn {tag, _} -> AMQP.Basic.ack(channel, tag) end)
        still_pending
    end
  end

  defp reschedule_acks(pending_consumer_acks, bound, last_published_tag) do
    {to_reschedule, keep} = split_at(pending_consumer_acks, bound)
    reschedule(to_reschedule, keep, last_published_tag)
  end

  defp reschedule([], acks, _new_bound) do
    acks
  end

  defp reschedule([{tag, _} | t], acks, new_bound) do
    reschedule(t, [{tag, new_bound} | acks], new_bound)
  end

  defp split_at(pending, lower_bound) do
    split_at_([], pending, lower_bound)
  end

  defp split_at_(higher, [], _) do
    {higher, []}
  end

  defp split_at_(higher, [{_, bound} = h | t] = lower, lower_bound) do
    if bound >= lower_bound do
      split_at_([h | higher], t, lower_bound)
    else
      {higher, lower}
    end
  end

  defp flush_pending(
         %{main_channel: main_channel, pending_publishes: pending_publishes} = state,
         interval
       ) do
    if main_channel != nil do
      limit = System.system_time(:second) - interval
      # select msg from ets where stamp <= limit
      messages =
        :ets.select(pending_publishes, [{{:_, :"$1", :"$2"}, [{:"=<", :"$1", limit}], [:"$2"]}])

      keys =
        :ets.select(pending_publishes, [{{:"$1", :_, :"$2"}, [{:"=<", :"$1", limit}], [:"$1"]}])

      :ets.select_delete(pending_publishes, [
        {{:_, :"$1", :"$2"}, [{:"=<", :"$1", limit}], [true]}
      ])

      %{last_published_tag: last_published_tag, pending_consumer_acks: pending_consumer_acks} =
        new_state = resend_messages(messages, state)

      lower_seqno = List.first(keys)

      %{
        new_state
        | pending_consumer_acks:
            reschedule_acks(pending_consumer_acks, lower_seqno, last_published_tag)
      }
    else
      state
    end
  end
end
