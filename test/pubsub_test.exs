defmodule SubscriptionTest do
  use ExUnit.Case
  alias Eventail.Business
  doctest Business

  defmodule TestHandler do
    @behaviour Eventail.Handlers

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
             events: ["Ping"],
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

  test "handle_local" do
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

  defp hosts do
    System.get_env("BROKERS", "amqp://localhost") |> String.split(",")
  end
end
