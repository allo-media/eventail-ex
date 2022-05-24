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

  test "publish simple log event", context do
    # Prepare test listener
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)
    {:ok, %{queue: test_queue}} = AMQP.Queue.declare(chan, "simple_queue", exclusive: true)
    AMQP.Queue.bind(chan, test_queue, Logging.exchange(), routing_key: "#")

    # Tested functions
    Logging.emergency(context.endpoint, "hello", "this is a test", "1234")
    Logging.alert(context.endpoint, "hello", "this is a test", "1234")
    Logging.critical(context.endpoint, "hello", "this is a test", "1234")
    Logging.error(context.endpoint, "hello", "this is a test", "1234")
    Logging.warning(context.endpoint, "hello", "this is a test", "1234")
    Logging.notice(context.endpoint, "hello", "this is a test", "1234")
    Logging.info(context.endpoint, "hello", "this is a test", "1234")
    Logging.debug(context.endpoint, "hello", "this is a test", "1234")

    # check
    # Give time to the messages to arrive in the queue
    Process.sleep(100)

    for {level, levelname} <- [
          {0, "EMERGENCY"},
          {1, "ALERT"},
          {2, "CRITICAL"},
          {3, "ERROR"},
          {4, "WARNING"},
          {5, "NOTICE"},
          {6, "INFO"},
          {7, "DEBUG"}
        ] do
      check_simple_log(chan, level, levelname)
    end
  end

  test "publish full log event", context do
    # Prepare test listener
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)
    {:ok, %{queue: test_queue}} = AMQP.Queue.declare(chan, "full_queue", exclusive: true)
    AMQP.Queue.bind(chan, test_queue, Logging.exchange(), routing_key: "#")

    # Tested functions
    Logging.emergency(context.endpoint, "hello", "this is a test", "1234",
      more_info: "personal field",
      anything: "that you want"
    )

    Logging.alert(context.endpoint, "hello", "this is a test", "1234",
      more_info: "personal field",
      anything: "that you want"
    )

    Logging.critical(context.endpoint, "hello", "this is a test", "1234",
      more_info: "personal field",
      anything: "that you want"
    )

    Logging.error(context.endpoint, "hello", "this is a test", "1234",
      more_info: "personal field",
      anything: "that you want"
    )

    Logging.warning(context.endpoint, "hello", "this is a test", "1234",
      more_info: "personal field",
      anything: "that you want"
    )

    Logging.notice(context.endpoint, "hello", "this is a test", "1234",
      more_info: "personal field",
      anything: "that you want"
    )

    Logging.info(context.endpoint, "hello", "this is a test", "1234",
      more_info: "personal field",
      anything: "that you want"
    )

    Logging.debug(context.endpoint, "hello", "this is a test", "1234",
      more_info: "personal field",
      anything: "that you want"
    )

    # check
    # Give time to the messages to arrive in the queue
    Process.sleep(100)

    for {level, levelname} <- [
          {0, "EMERGENCY"},
          {1, "ALERT"},
          {2, "CRITICAL"},
          {3, "ERROR"},
          {4, "WARNING"},
          {5, "NOTICE"},
          {6, "INFO"},
          {7, "DEBUG"}
        ] do
      check_full_log(chan, level, levelname)
    end
  end

  defp check_full_log(chan, level, levelname) do
    {:ok, payload, _meta} = AMQP.Basic.get(chan, "full_queue")
    gelf = Jason.decode!(payload)
    assert gelf["short_message"] == "hello"
    assert gelf["full_message"] == "this is a test"
    assert gelf["_conversation_id"] == "1234"
    assert gelf["_levelname"] == levelname
    assert gelf["level"] == level
    assert gelf["_more_info"] == "personal field"
    assert gelf["_anything"] == "that you want"
  end

  defp check_simple_log(chan, level, levelname) do
    {:ok, payload, _meta} = AMQP.Basic.get(chan, "simple_queue")
    gelf = Jason.decode!(payload)
    assert gelf["short_message"] == "hello"
    assert gelf["full_message"] == "this is a test"
    assert gelf["_conversation_id"] == "1234"
    assert gelf["_levelname"] == levelname
    assert gelf["level"] == level
  end

  defp hosts do
    System.get_env("BROKERS", "amqp://localhost") |> String.split(",")
  end
end
