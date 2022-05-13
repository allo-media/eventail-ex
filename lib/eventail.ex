defmodule Eventail do
  @moduledoc """
  Allo-Media Event bus connector Eventail for Elixir.
  """

  alias AMQP.Channel
  alias AMQP.Connection
  require Logger
  use GenServer

  @reconnect_interval 2_000
  @heartbeat 5

  @doc """
  Start an AMQP Connection supervisor.
  * `hosts`: a list of rabbitmq hosts (from the same cluster)
  * `name`: the name of this connection
  * `on_connect`: a tuple {module, function identifier}; the function will be called with the connection as parameter
  each time we successfuly (re-)connect.
  """
  @spec start_link(hosts: list(String.t()), name: atom(), on_connect: {module(), atom()}) ::
          {:ok, pid} | {:error, term} | :ignore
  def start_link(hosts: hosts, name: name, on_connect: mf) do
    GenServer.start_link(__MODULE__, {hosts, mf}, name: name)
  end

  @spec open_channel(atom()) :: {:ok, Channel.t()} | {:error, any()}
  def open_channel(name) do
    _open_channel(name)
  catch
    :exit, _ -> {:error, :not_connected}
  end

  @spec close_channel(Channel.t()) :: :ok | {:error, AMQP.Basic.error()}
  def close_channel(chan) do
    Channel.close(chan)
  end

  @impl GenServer
  def init({hosts, on_connect}) do
    send(self(), :connect)
    {:ok, {hosts, on_connect, nil}}
  end

  @impl GenServer
  def handle_call(:get, _, {_, _, conn} = state) do
    {:reply, conn, state}
  end

  @impl GenServer
  def handle_info(:connect, {hosts, {mod, func}, _}) do
    case connect(hosts) do
      {:ok, conn} ->
        # Get notifications when the connection goes down
        Logger.info("Connected to broker!")
        Process.monitor(conn.pid)
        apply(mod, func, [conn])
        {:noreply, {hosts, {mod, func}, conn}}

      {:error, reason} ->
        Logger.error(
          "Failed to connect to a broker for reason #{inspect(reason)}. Reconnecting later..."
        )

        # Retry later
        Process.send_after(self(), :connect, @reconnect_interval)
        {:noreply, {hosts, {mod, func}, nil}}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _, :process, _pid, reason}, _) do
    Logger.error("Broker disconnected (#{inspect(reason)}). Reconnecting...")
    # As we are supervised, close and let the supervisor restart us.
    # This will be useful later when we link to consumer processes to ensure all
    # the tree is restarted when we lose the connection to the EDA.
    {:stop, {:connection_lost, reason}, nil}
  end

  defp connect([]) do
    {:error, "All known brokers are unavailable"}
  end

  defp connect([host | hosts]) do
    case Connection.open(host, heartbeat: @heartbeat) do
      {:ok, conn} ->
        {:ok, conn}

      {:error, reason} ->
        Logger.error("Connection to #{host} failed for reason #{inspect(reason)}")
        connect(hosts)
    end
  end

  defp _open_channel(name) do
    with conn when not is_nil(conn) <- GenServer.call(name, :get),
         {:ok, chan} <- Channel.open(conn) do
      {:ok, chan}
    else
      nil -> {:error, :not_connected}
      err -> err
    end
  end
end
