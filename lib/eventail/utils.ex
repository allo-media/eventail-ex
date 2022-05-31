defmodule Eventail.Utils do
  @moduledoc """
  Internal convenient functions
  """

  @doc false
  def bind(_, _, _, []) do
    :ok
  end

  @doc false
  def bind(channel, queue, exchange, [key | rest]) do
    :ok = AMQP.Queue.bind(channel, queue, exchange, routing_key: key)
    bind(channel, queue, exchange, rest)
  end

  @doc false
  def decode_body(body, "application/cbor") do
    case CBOR.decode(body) do
      {:ok, payload, _rest} -> {:ok, payload}
      err -> err
    end
  end

  @doc false
  def decode_body(body, _) do
    Jason.decode(body)
  end
end
