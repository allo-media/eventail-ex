defmodule SubscriptionTest do
  use ExUnit.Case
  alias Eventail.Business
  doctest Business

  defmodule TestHandler do
    @behaviour Eventail.Handlers

    @impl
    def handle_event(endpoint, event, payload, conversation_id, %{app_context: test_pid}) do
      send(test_pid, {event, payload, conversation_id})
      :ok
    end

    @impl
    def handle_command(endpoint, event, payload, conversation_id, reply_to, correlation_id, %{
          app_context: test_pid
        }) do
      send(test_pid, {event, payload, conversation_id, reply_to, correlation_id})
      :ok
    end

    @impl
    def handle_result(endpoint, key, payload, conversation_id, status, correlation_id, %{
          app_context: test_pid
        }) do
      send(test_pid, {key, payload, conversation_id, status, correlation_id})
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
           name: :log_test,
           setup: [
             exclusive_queues: true,
             events: ["Ping"],
             handlers: TestHandler,
             app_context: self()
           ]
         ]},
        restart: :temporary
      )

    {:ok, endpoint: Eventail.get_endpoint(pid)}
  end

  test "handle event", context do
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    AMQP.Basic.publish(chan, Business.event_exchange(), "Ping", CBOR.encode("hello"),
      headers: [{"conversation_id", :longstr, "1234"}],
      content_type: "application/cbor"
    )

    assert_receive {"Ping", "hello", "1234"}
  end

  defp hosts do
    System.get_env("BROKERS", "amqp://localhost") |> String.split(",")
  end
end
