defmodule EventailTest do
  use ExUnit.Case
  doctest Eventail

  test "greets the world" do
    assert Eventail.hello() == :world
  end
end
