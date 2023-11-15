defmodule ConfigurationTest do
  use ExUnit.Case
  alias Eventail.Business
  alias Eventail.Config
  doctest Config

  defmodule TestHandler do
    @behaviour Eventail.Handlers

    def handle_event(_, "Malicious", _, _, _) do
      raise "failed to handle event"
    end

    def handle_event(_endpoint, event, payload, conversation_id, %{app_context: test_pid}) do
      send(test_pid, {event, payload, conversation_id})
      {:ok, test_pid}
    end

    def handle_command(_endpoint, event, payload, conversation_id, reply_to, correlation_id, %{
          app_context: test_pid
        }) do
      send(test_pid, {event, payload, conversation_id, reply_to, correlation_id})
      {:ok, test_pid}
    end

    def handle_result(_endpoint, key, payload, conversation_id, status, correlation_id, %{
          app_context: test_pid
        }) do
      send(test_pid, {key, payload, conversation_id, status, correlation_id})
      {:ok, test_pid}
    end

    def handle_config(_endpoint, "UsersUpdated" = config, payload, %{app_context: test_pid}) do
      send(test_pid, {config, payload})
      {:wait, test_pid}
    end

    def handle_config(_endpoint, "CredentialsUpdated" = config, payload, %{app_context: test_pid}) do
      send(test_pid, {config, payload})
      {:ok, test_pid}
    end

    def handle_local(_endpoint, :ready, %{app_context: test_pid}) do
      send(test_pid, :ready)
      {:ok, test_pid}
    end

    def handle_local(_endpoint, :waiting_for_config, %{app_context: test_pid}) do
      send(test_pid, :waiting_for_config)
      {:ok, test_pid}
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
             commands: ["Echo"],
             configurations: ["UsersUpdated", "CredentialsUpdated"],
             handlers: TestHandler,
             app_context: self()
           ]
         ]},
        restart: :temporary
      )

    {:ok, endpoint: Eventail.get_endpoint(pid)}
  end

  test "waiting for config" do
    assert_receive :waiting_for_config
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    :ok =
      AMQP.Basic.publish(chan, Business.event_exchange(), "Ping", CBOR.encode("hello"),
        headers: [{"conversation_id", :longstr, "1234"}],
        content_type: "application/cbor"
      )

    refute_receive _
  end

  test "partial config", context do
    assert_receive :waiting_for_config
    Config.publish(context.endpoint, "UsersUpdated", "hello")
    assert_receive {"UsersUpdated", "hello"}
    # Prepare test simple publish
    {:ok, conn} = AMQP.Connection.open(hosts() |> List.first())

    {:ok, chan} = AMQP.Channel.open(conn)

    :ok =
      AMQP.Basic.publish(chan, Business.event_exchange(), "Ping", CBOR.encode("hello"),
        headers: [{"conversation_id", :longstr, "1234"}],
        content_type: "application/cbor"
      )

    refute_receive _
  end

  test "final config", context do
    assert_receive :waiting_for_config
    Config.publish(context.endpoint, "CredentialsUpdated", "secret")
    assert_receive {"CredentialsUpdated", "secret"}
    assert_receive :ready
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

  test "config during normal operations", context do
    assert_receive :waiting_for_config
    Config.publish(context.endpoint, "CredentialsUpdated", "secret")
    assert_receive {"CredentialsUpdated", "secret"}
    assert_receive :ready
    # Check that `:wait` is ignored during normal operations
    Config.publish(context.endpoint, "UsersUpdated", "users")
    assert_receive {"UsersUpdated", "users"}

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

  defp hosts do
    System.get_env("BROKERS", "amqp://localhost") |> String.split(",")
  end
end
