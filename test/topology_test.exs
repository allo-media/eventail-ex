defmodule TopologyTest do
  use ExUnit.Case
  doctest Eventail

  test "start events only" do
    {:ok, pid} =
      Eventail.start_link(
        hosts: hosts(),
        name: :events_only,
        setup: [
          events: ["PlayPressed", "StopPressed"]
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
          commands: ["Echo", "Add"]
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
          commands: ["Echo"]
        ]
      )

    assert %Eventail.Endpoint{pid: ^pid, logical_service: :private_queues, hostname: _} =
             Eventail.get_endpoint(:private_queues)
  end

  defp hosts do
    System.get_env("BROKERS", "amqp://localhost") |> String.split(",")
  end
end
