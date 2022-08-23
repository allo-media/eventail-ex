defmodule Eventail.Config do
  @moduledoc """
  EDA Configuration API
  """
  require Logger

  alias Eventail.Endpoint
  alias Eventail.Utils

  @exchange_type :topic
  #
  # Public API
  #

  @doc """
  Publish a configuration event (request or configuration change).
  """
  @spec publish(Endpoint.t(), String.t(), map) :: :ok
  def publish(endpoint, event, payload) do
    GenServer.cast(
      endpoint.pid,
      {:emit_config, event, CBOR.encode(payload), content_type: "application/cbor"}
    )
  end

  #
  # Internal API
  #

  @doc false
  @spec setup(AMQP.Channel.t(), String.t()) :: :ok
  def setup(channel, exchange) do
    # Configuration is quite "intimate" to the endpoint, to we let it set the exchange name
    # so it can filter incoming messages more easily.
    :ok = AMQP.Exchange.declare(channel, exchange, @exchange_type, durable: true)
  end

  @spec consume(AMQP.Channel.t(), String.t(), map()) :: String.t()
  def consume(channel, exchange, setup) do
    {:ok, %{queue: config_q}} =
      AMQP.Queue.declare(channel, "",
        exclusive: true,
        durable: false
      )

    Utils.bind(channel, config_q, exchange, setup.configurations)
    Logger.info("Expecting configuration events #{inspect(setup.configurations)}")
    {:ok, conf_constag} = AMQP.Basic.consume(channel, config_q)
    conf_constag
  end

  @doc false
  @spec handle_message(Eventail.Endpoint.t(), AMQP.Channel.t(), binary(), map(), module()) ::
          {:ok, map()} | {:wait, map()}
  def handle_message(
        endpoint,
        channel,
        body,
        %{
          routing_key: routing_key,
          content_type: content_type,
          delivery_tag: tag
        } = context,
        handlers
      ) do
    {:ok, payload} = Utils.decode_body(body, content_type)
    # Regarding configuration, we apply the "let it crash" policy, which is the safest:
    # we don't want misconfigured applications. So we can ACK the message before processing it,
    AMQP.Basic.ack(channel, tag)
    handlers.handle_config(endpoint, routing_key, payload, context)
  end
end
