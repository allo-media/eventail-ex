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
           name: :log_test,
           exclusive_queues: true,
           events: [],
           commands: []
         ]},
        restart: :temporary
      )

    {:ok, endpoint: Eventail.get_endpoint(pid)}
  end

  test "publish log event", context do
    Logging.info(context.endpoint, "hello", "this is a test", "1234",
      more_info: "personal field",
      anything: "that you want"
    )

    Process.sleep(500)
  end

  defp hosts do
    System.get_env("BROKERS", "amqp://localhost") |> String.split(",")
  end
end
