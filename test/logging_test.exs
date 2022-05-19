defmodule LoggingTest do
  use ExUnit.Case
  alias Eventail.Logging
  doctest Logging

  setup do
    pid =
      start_supervised!(
        {Eventail,
         [
           hosts: hosts(),
           logical_service: :log_test_service,
           name: :log_test,
           setup: [exclusive_queues: true]
         ]},
        restart: :temporary
      )

    {:ok, endpoint: Eventail.get_endpoint(pid)}
  end

  test "publish log event", context do
    # Prepare test listener
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)
    {:ok, %{queue: test_queue}} = AMQP.Queue.declare(chan, "test_queue", exclusive: true)
    AMQP.Queue.bind(chan, test_queue, Logging.exchange(), routing_key: "#")

    # Tested function
    Logging.info(context.endpoint, "hello", "this is a test", "1234",
      more_info: "personal field",
      anything: "that you want"
    )

    # check
    # Give time to the message to arrive in the queue
    Process.sleep(100)
    {:ok, payload, meta} = AMQP.Basic.get(chan, "test_queue")
    gelf = Jason.decode!(payload)
    assert gelf["short_message"] == "hello"
    assert gelf["full_message"] == "this is a test"
    assert gelf["_conversation_id"] == "1234"
    assert gelf["_more_info"] == "personal field"
    assert gelf["_anything"] == "that you want"
  end

  defp hosts do
    System.get_env("BROKERS", "amqp://localhost") |> String.split(",")
  end
end
