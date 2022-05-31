defmodule SubscriptionTest do
  use ExUnit.Case
  alias Eventail.Business
  doctest Business

  defmodule TestHandler do
    @behaviour Eventail.Handlers

    def handle_event(_, "Malicious", _, _, _) do
      raise "failed to handle event"
    end

    def handle_event(_endpoint, event, payload, conversation_id, %{app_context: test_pid}) do
      send(test_pid, {event, payload, conversation_id})
      :ok
    end

    def handle_command(_endpoint, event, payload, conversation_id, reply_to, correlation_id, %{
          app_context: test_pid
        }) do
      send(test_pid, {event, payload, conversation_id, reply_to, correlation_id})
      :ok
    end

    def handle_result(_endpoint, key, payload, conversation_id, status, correlation_id, %{
          app_context: test_pid
        }) do
      send(test_pid, {key, payload, conversation_id, status, correlation_id})
      :ok
    end

    def handle_local(endpoint, :ready, _) do
      Eventail.publish_local(endpoint, :health, 1000)
    end

    def handle_local(_endpoint, :health, %{app_context: test_pid}) do
      send(test_pid, :health)
    end

    def handle_local(_, local_event, %{app_context: test_pid}) do
      send(test_pid, local_event)
    end

    def handle_config(_, _, _, _) do
      :ok
    end
  end

  setup do
    pid =
      start_supervised!(
        {Eventail,
         [
           hosts: hosts(),
           logical_service: :test_service,
           name: :subscription_test,
           setup: [
             exclusive_queues: true,
             events: ["Ping", "Malicious"],
             commands: ["Echo", "test.Result"],
             handlers: TestHandler,
             app_context: self(),
             concurrent_handlers: true
           ]
         ]},
        restart: :temporary
      )

    {:ok, endpoint: Eventail.get_endpoint(pid)}
  end

  test "handle event" do
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    :ok =
      AMQP.Basic.publish(chan, Business.event_exchange(), "Ping", CBOR.encode("hello"),
        headers: [{"conversation_id", :longstr, "1234"}],
        content_type: "application/cbor"
      )

    assert_receive {"Ping", "hello", "1234"}
  end

  test "handle json event" do
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    :ok =
      AMQP.Basic.publish(chan, Business.event_exchange(), "Ping", Jason.encode!("hello"),
        headers: [{"conversation_id", :longstr, "1234"}],
        content_type: "application/json"
      )

    assert_receive {"Ping", "hello", "1234"}
  end

  test "undecodable payload" do
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    :ok =
      AMQP.Basic.publish(chan, Business.event_exchange(), "Ping", "boom!",
        headers: [{"conversation_id", :longstr, "1234"}],
        content_type: "application/json"
      )

    refute_receive _
  end

  test "missing conversation_id" do
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    :ok =
      AMQP.Basic.publish(chan, Business.event_exchange(), "Ping", CBOR.encode("hello"),
        content_type: "application/cbor",
        headers: []
      )

    refute_receive _
  end

  test "missing headers" do
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    :ok =
      AMQP.Basic.publish(chan, Business.event_exchange(), "Ping", CBOR.encode("hello"),
        content_type: "application/cbor"
      )

    refute_receive _
  end

  test "crashed handler" do
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    # Prepare log listener
    {:ok, %{queue: test_queue}} = AMQP.Queue.declare(chan, "log_queue", exclusive: true)
    AMQP.Queue.bind(chan, test_queue, Eventail.Logging.exchange(), routing_key: "*.EMERGENCY")

    :ok =
      AMQP.Basic.publish(chan, Business.event_exchange(), "Malicious", CBOR.encode("hello"),
        headers: [{"conversation_id", :longstr, "1234"}],
        content_type: "application/cbor"
      )

    refute_receive _

    # check
    # Give time to the logs messages to arrive in the queue
    Process.sleep(100)
    {:ok, _payload, _meta} = AMQP.Basic.get(chan, "log_queue")
  end

  test "handle command" do
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    :ok =
      AMQP.Basic.publish(chan, Business.command_exchange(), "Echo", CBOR.encode("hello"),
        headers: [{"conversation_id", :longstr, "1234"}],
        content_type: "application/cbor",
        correlation_id: "c1",
        reply_to: "test.Result"
      )

    assert_receive {"Echo", "hello", "1234", "test.Result", "c1"}
  end

  test "bad command" do
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    :ok =
      AMQP.Basic.publish(chan, Business.command_exchange(), "Echo", CBOR.encode("hello"),
        headers: [{"conversation_id", :longstr, "1234"}],
        content_type: "application/cbor"
      )

    refute_receive _
  end

  test "handle result" do
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    :ok =
      AMQP.Basic.publish(chan, Business.command_exchange(), "test.Result", CBOR.encode("hello"),
        headers: [{"conversation_id", :longstr, "1234"}, {"status", :longstr, "success"}],
        content_type: "application/cbor",
        correlation_id: "c1"
      )

    assert_receive {"test.Result", "hello", "1234", "success", "c1"}
  end

  test "handle_local", context do
    Eventail.publish_local(context.endpoint, :local_test_delay, 10)
    Eventail.publish_local(context.endpoint, :local_test, 0)
    assert_receive :local_test
    assert_receive :local_test_delay
    # Just wait for health check
    assert_receive :health, 1100
  end

  test "successfully publish event", context do
    Business.publish_event(context.endpoint, "Ping", "what's up?", "321")
    assert_receive {"Ping", "what's up?", "321"}
    Business.publish_event(context.endpoint, "Ping", "what's up?", "321", true)
    assert_receive {"Ping", "what's up?", "321"}
  end

  test "successfully send command", context do
    Business.send_command(context.endpoint, "Echo", "eh ho!", "321", "test.Result", "recon1")
    assert_receive {"Echo", "eh ho!", "321", "test.Result", "recon1"}

    Business.send_command(
      context.endpoint,
      "Echo",
      "eh ho!",
      "321",
      "test.Result",
      "recon1",
      true
    )

    assert_receive {"Echo", "eh ho!", "321", "test.Result", "recon1"}
  end

  test "successfully respond", context do
    Business.return_success(context.endpoint, "test.Result", "!oh he", "321", "recon1")
    assert_receive {"test.Result", "!oh he", "321", "success", "recon1"}
    Business.return_error(context.endpoint, "test.Result", "!oh he", "321", "recon1")
    assert_receive {"test.Result", "!oh he", "321", "error", "recon1"}
    Business.return_success(context.endpoint, "test.Result", "!oh he", "321", "recon1", true)
    assert_receive {"test.Result", "!oh he", "321", "success", "recon1"}
  end

  test "massive send", context do
    # to trigger multiple acks
    Business.publish_event(context.endpoint, "Ping", "what's up?", "321")
    Business.publish_event(context.endpoint, "Ping", "what's up?", "321")
    Business.publish_event(context.endpoint, "Ping", "what's up?", "321")
    Business.publish_event(context.endpoint, "Ping", "what's up?", "321")
    Business.publish_event(context.endpoint, "Ping", "what's up?", "321")
    Business.publish_event(context.endpoint, "Ping", "what's up?", "321")
    Business.publish_event(context.endpoint, "Ping", "what's up?", "321")
    Business.publish_event(context.endpoint, "Ping", "what's up?", "321")
    Business.publish_event(context.endpoint, "Ping", "what's up?", "321")
    Process.sleep(100)
    size = Eventail.get_pending_confirms(context.endpoint)
    assert size == 0
  end

  test "single publisher nack" do
    pending = :ets.new(:test_table, [:public])

    :ets.insert(
      pending,
      {1, System.system_time(:second),
       {:publish, Business.event_exchange(), "Tested", "mesg1", []}}
    )

    :ets.insert(
      pending,
      {2, System.system_time(:second),
       {:publish, Business.event_exchange(), "Tested", "mesg2", []}}
    )

    :ets.insert(
      pending,
      {3, System.system_time(:second),
       {:publish, Business.event_exchange(), "Tested", "mesg3", []}}
    )

    query = [{{:"$1", :_, :"$2"}, [{:>, :"$1", 1}], [:"$2"]}]
    msgs = :ets.select(pending, query)
    assert length(msgs) == 2
    Eventail.handle_info({:basic_nack, 2, false}, %{pending_ack: pending, business_channel: nil})
    msgs2 = :ets.select(pending, query)
    assert length(msgs2) == 1
    msgs3 = :ets.select(pending, [{{:"$1", :_, :"$2"}, [{:"=<", :"$1", 2}], [:"$2"]}])
    assert length(msgs3) == 2
  end

  test "multiple publisher nack" do
    pending = :ets.new(:test_table, [:public])

    :ets.insert(
      pending,
      {1, System.system_time(:second),
       {:publish, Business.event_exchange(), "Tested", "mesg1", []}}
    )

    :ets.insert(
      pending,
      {2, System.system_time(:second),
       {:publish, Business.event_exchange(), "Tested", "mesg2", []}}
    )

    :ets.insert(
      pending,
      {3, System.system_time(:second),
       {:publish, Business.event_exchange(), "Tested", "mesg3", []}}
    )

    query = [{{:"$1", :_, :"$2"}, [{:>, :"$1", 0}], [:"$2"]}]
    msgs = :ets.select(pending, query)
    assert length(msgs) == 3
    Eventail.handle_info({:basic_nack, 2, true}, %{pending_ack: pending, business_channel: nil})
    msgs2 = :ets.select(pending, query)
    assert length(msgs2) == 1
    assert_receive {:"$gen_cast", {:publish, _, "Tested", "mesg1", []}}
    assert_receive {:"$gen_cast", {:publish, _, "Tested", "mesg2", []}}
  end

  test "pending ack collector" do
    pending = :ets.new(:test_table, [:public])

    :ets.insert(
      pending,
      {1, System.system_time(:second) - 10,
       {:publish, Business.event_exchange(), "Tested", "mesg1", []}}
    )

    :ets.insert(
      pending,
      {2, System.system_time(:second) - 9,
       {:publish, Business.event_exchange(), "Tested", "mesg2", []}}
    )

    :ets.insert(
      pending,
      {3, System.system_time(:second) - 8,
       {:publish, Business.event_exchange(), "Tested", "mesg3", []}}
    )

    query = [{{:"$1", :_, :"$2"}, [{:>, :"$1", 0}], [:"$2"]}]
    msgs = :ets.select(pending, query)
    assert length(msgs) == 3
    Eventail.handle_info(:check_pending, %{pending_ack: pending, business_channel: 123})
    msgs2 = :ets.select(pending, query)
    assert Enum.empty?(msgs2)
    assert_receive {:"$gen_cast", {:publish, _, "Tested", "mesg1", []}}
    assert_receive {:"$gen_cast", {:publish, _, "Tested", "mesg2", []}}
    assert_receive {:"$gen_cast", {:publish, _, "Tested", "mesg3", []}}
  end

  defp hosts do
    System.get_env("BROKERS", "amqp://localhost") |> String.split(",")
  end
end
