defmodule TopologyTest do
  use ExUnit.Case
  doctest Eventail

  defmodule NOPHandler do
    @behaviour Eventail.Handlers

    @impl
    def handle_event(_arg0, _arg1, _term, _arg3, _map) do
      :ok
    end

    @impl
    def handle_command(_arg0, _arg1, _term, _arg3, _arg4, _arg5, _map) do
      :ok
    end

    @impl
    def handle_result(_arg0, _arg1, _term, _arg3, _arg4, _arg5, _map) do
      :ok
    end
  end

  test "start events only" do
    {:ok, pid} =
      Eventail.start_link(
        hosts: hosts(),
        name: :events_only,
        setup: [
          events: ["PlayPressed", "StopPressed"],
          handlers: NOPHandler
        ]
      )

    assert %Eventail.Endpoint{pid: ^pid, logical_service: :events_only, hostname: _} =
             Eventail.get_endpoint(:events_only)
  end

  test "start commands only" do
    {:ok, pid} =
      Eventail.start_link(
        hosts: hosts(),
        name: :cmd_only,
        setup: [
          commands: ["Echo", "Add"],
          handlers: NOPHandler
        ]
      )

    assert %Eventail.Endpoint{pid: ^pid, logical_service: :cmd_only, hostname: _} =
             Eventail.get_endpoint(:cmd_only)
  end

  test "private, transient queues" do
    {:ok, pid} =
      Eventail.start_link(
        hosts: hosts(),
        name: :private_queues,
        setup: [
          exclusive_queues: true,
          events: ["PlayPressed", "StopPressed"],
          commands: ["Echo"],
          handlers: NOPHandler
        ]
      )

    assert %Eventail.Endpoint{pid: ^pid, logical_service: :private_queues, hostname: _} =
             Eventail.get_endpoint(:private_queues)
  end

  test "missing handlers despite subscriptions" do
    assert_raise RuntimeError, "Missing handlers for subscriptions!", fn ->
      Eventail.start_link(
        hosts: hosts(),
        name: :private_queues,
        setup: [
          exclusive_queues: true,
          events: ["PlayPressed", "StopPressed"],
          commands: ["Echo"]
        ]
      )
    end
  end

  defp hosts do
    System.get_env("BROKERS", "amqp://localhost") |> String.split(",")
  end
end
