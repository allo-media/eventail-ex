# Eventail for Elixir

An idiomatic library and GenServer to develop Elixir services on the EDA.

As an Eventail endpoint is just an Elixir process, an Elixir application can host multiple logical EDA services, and it
can scale each of them with as many workers as it needs to. Please keep in mind, though, that incoming messages can be processed in parallel in their own processes, so multiple workers for a logical service are only needed for redundancy or for high data throughput, not for computing power.

This documentation is about the Elixir API specifics; the reader should already master the EDA principles.

[Full documentation](https://eventail-elixir.netlify.app/) on Netlify.

## Installation

Add Eventail as a dependency to your `mix.exs` file.

```elixir
def deps do
  [
    {:eventail, git: "https://e58fb7a11af519cbaafd02f4778dbdd9d173595c@github.com/allo-media/eventail-ex.git", tag: "1.0.0"}
  ]
end
```

## Quick start

First, you need to write the message handlers for your service endpoint, by implementing the `Eventail.Handlers` behavior.

You can use pattern matching in order to have at least one function clause per subscribed message.
You don't need to expect and handle messages you won't subscribe too.

As an example, we'll implement an Echo utility service that responds to `Echo` commands by returning the input
payload message, reversed.

Example:

```elixir
defmodule Echo do
  @behaviour Eventail.Handlers

  alias Eventail.Business
  alias Eventail.Logging

  def handle_event(_endpoint, _event, _payload, _conversation_id, _context) do
    # No event subscriptions
    :ok
  end

  def handle_command(
        endpoint,
        "echo.EchoMessage",
        %{"message" => msg},
        conversation_id,
        reply_to,
        correlation_id,
        _context
      ) do
    Logging.info(endpoint, "Received Echo command", "Message to echo = #{msg}", conversation_id)

    Business.return_success(
      endpoint,
      reply_to,
      %{echo: String.reverse(msg)},
      conversation_id,
      correlation_id
    )
  end

  def handle_result(_endpoint, _key, _payload, _conversation_id, _status, _correlation_id, _context) do
    # No results expected
    :ok
  end

  def handle_local(endpoint, :ready, _context) do
    Eventail.publish_local(endpoint, :health)
    :ok
  end

  def handle_local(endpoint, :health, _context) do
    Logging.notice(endpoint, "Echo health check", "I'm alive!", UUID.uuid4(),
      custom_gelf: "for the fun",
      another_custom_gelf_field: 123
    )

    # Next health message in one minute
    Eventail.publish_local(endpoint, :health, 60_000)
    :ok
  end

  def handle_config(_, _, _, %{app_context: app_context}) do
    # ignore configuration messages for now
    {:ok, app_context}
  end
end
```
The example also illustrates how to use the local events to implement the periodic health check.

As you see, every handler takes an endpoint descriptor as input, as do all Eventail API functions.

For more fun, we'll need another service to send the echo commands and receive the results:

```elixir
defmodule Ping do
  @behaviour Eventail.Handlers

  alias Eventail.Logging

  def handle_event(_endpoint, _event, _payload, _conversation_id, _context) do
    # No event subscriptions
    :ok
  end

  def handle_command(
        _endpoint,
        _cmd,
        _payload,
        _conversation_id,
        _reply_to,
        _correlation_id,
        _context
      ) do
    :ok
  end

  def handle_result(endpoint, "ping.Result", %{"echo" => msg}, conversation_id, "success", correlation_id, _context) do
    Logging.info(endpoint, "Received echo", "Echo = #{msg}", conversation_id, correlation_id: correlation_id)
    :ok
  end

  def handle_local(_endpoint, _event, _context) do
    # ignore local messages
    :ok
  end

  def handle_config(_, _, _, %{app_context: app_context}) do
    # ignore configuration messages for now
    {:ok, app_context}
  end
end
```

Now, let's see interactively how to start the endpoints and play with them!

```
iex(1)> Eventail.start_link(
  hosts: ["amqp://localhost"],
  logical_service: :echo,
  name: :echo_service,
  setup: [
    commands: ["echo.EchoMessage"],
    handlers: Echo
  ])
{:ok, #PID<0.327.0>}
iex(2)>
17:26:07.476 [info]  Connected to broker!

17:26:07.501 [debug] Application echo logged on EDA: %{:full_message => "I'm alive!", :host => "echo@rtxmpro", :level => 5, :short_message => "Echo health check", :timestamp => 1654010767.496, :version => "1.1", "_another_custom_gelf_field" => 123, "_conversation_id" => "8ee495c3-913d-4c4c-946a-e7fc30409438", "_custom_gelf" => "for the fun", "_levelname" => :NOTICE, "_logical_service" => :echo, "_worker_pid" => "#PID<0.353.0>"}

nil
iex(3)> Eventail.start_link(
  hosts: ["amqp://localhost"],
  logical_service: :ping,
  name: :ping_service,
  setup: [
    commands: ["ping.Result"],
    handlers: Ping
  ])
{:ok, #PID<0.355.0>}
iex(4)>
17:26:14.160 [info]  Connected to broker!

nil
iex(5)> ping_endpoint = Eventail.get_endpoint(:ping_service)
%Eventail.Endpoint{
  hostname: "rtxmpro",
  logical_service: :ping,
  pid: #PID<0.355.0>
}
iex(6)> Eventail.Business.send_command(
  ping_endpoint, "echo.EchoMessage", %{message: "hello world!"}, UUID.uuid4(), "ping.Result", "corr1")
:ok
iex(7)>
17:26:39.377 [debug] Application echo logged on EDA: %{:full_message => "Message to echo = hello world!", :host => "echo@rtxmpro", :level => 6, :short_message => "Received Echo command", :timestamp => 1654010799.377, :version => "1.1", "_conversation_id" => "7fd812bc-117e-4c97-973e-d5fd846fe34e", "_levelname" => :INFO, "_logical_service" => :echo, "_worker_pid" => "#PID<0.327.0>"}

17:26:39.381 [debug] Application ping logged on EDA: %{:full_message => "Echo = !dlrow olleh", :host => "ping@rtxmpro", :level => 6, :short_message => "Received echo", :timestamp => 1654010799.38, :version => "1.1", "_conversation_id" => "7fd812bc-117e-4c97-973e-d5fd846fe34e", "_correlation_id" => "corr1", "_levelname" => :INFO, "_logical_service" => :ping, "_worker_pid" => "#PID<0.355.0>"}

nil
```


See also:
  * `t:Eventail.config/0`
  * `Eventail.start_link/1`
  * `Eventail.get_endpoint/1`
  * `Eventail.publish_local/3`


## Supervision

The endpoint process is monitoring the EDA connection and automatically reconnects without crashing, so that
no pending messages are lost, and existing endpoint descriptors are still valid.

However, the endpoint can crash in other circumstances, in particular if something goes wrong during the
configuration step.

In a production application, the endpoint should be started under the supervision tree.

```elixir
defmodule Echo.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Supervisor.child_spec(
        {Eventail,
         [
           hosts: ["amqp://localhost"],
           logical_service: :configurator,
           name: :config_service,
           setup: [
             configurations: ["EchoSettingsRequested", "ConfiguratorStarted"],
             handlers: Configurator
           ]
         ]},
        id: :config_service
      )

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Echo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

## Configuration from the EDA

**This is experimental, as no blueprint on the subject has been devised yet**.

A new exchange has been added for configuration updates and requests: the `configurations` exchange, so that
configuration updates and requests have their own name space and "bus".

If an application subscribes to configuration updates, that triggers a new endpoint behavior (in the sense of
startup sequence, not Elixir interface!).

In that case, the endpoint won't join the business event bus (the `events` exchange) immediately, but it'll wait
until the application receives configuration updates and indicates it is ready to proceed.

This is the complete startup sequence:

```plain
Endpoint                                              Application Handlers
   |                                                            |
start                                                           |
   |                                                            |
join the logging bus                                            |
   |                                                            |
join the configuration bus                                      |
   |                                                            |
emit the :waiting_for_config local event        ->          handle_local
   |                                                            |
   |                                                        publish one or more configuration requests
   |                                                            |
receive configuration update                    ->          handle_config
   |                                            <-           returns "wait for more"
              [repeat the last step zero or more times]
   |                                                            |
receive configuration update                    ->          handle_config
   |                                            <-           returns "config ok"
   |                                                            |
join the business bus                                           |
   |
emit the :ready local event                     ->          handle_local
```

### Example

Let's update our Echo handler so that the string reversing can be configured from the EDA (on/off).

Add this to the `Echo` module:

```elixir
  def handle_local(endpoint, :waiting_for_config, _context) do
    # The naming convention is not defined yet.
    # Should it be `echo.SettingsRequested`, `EchoSettings.needed`…
    Config.publish(endpoint, "EchoSettingsRequested", %{})
  end

  def handle_config(endpoint, "EchoSettingsUpdated", %{"reverse" => reverse?}, _) do
    # or should it be `echo.SettingsUpdated` or `EchoSettings.updated`…
    # set and start
    Logging.info(endpoint, "Echo configured", "reverse = #{reverse?}", "conf")
    {:ok, %{reverse: reverse?}}
  end
```

Let's create a configuration server to serve the configuration on the EDA:

```elixir
defmodule Configurator do
  @behaviour Eventail.Handlers

  alias Eventail.Config

  def handle_event(_endpoint, _event, _payload, _conversation_id, _context) do
    # No event subscriptions
    :ok
  end

  def handle_command(
        _endpoint,
        _cmd,
        _payload,
        _conversation_id,
        _reply_to,
        _correlation_id,
        _context
      ) do
    :ok
  end

  def handle_result(_endpoint, _key, _payload, _conversation_id, _status, _correlation_id, _context) do
    # No results expected
    :ok
  end

  def handle_local(endpoint, :waiting_for_config, _context) do
    # The Eventail library is mostly used to develop business services, much less often configuration servers.
    # So we didn't bloat the library with additional options for the latter.
    # If your configuration server needs to subscribe to the Business bus too, we use this trick:
    # we send a configuration request to ourselves.
    Config.publish(endpoint, "ConfiguratorStarted", %{})
  end

  def handle_local(_endpoint, _event, _context) do
    # ignore other local messages
    :ok
  end

  def handle_config(_endpoint, "ConfiguratorStarted", _, %{app_context: app_context}) do
    # Consume our own request to join the business bus
    {:ok, app_context}
  end

  def handle_config(endpoint, "EchoSettingsRequested", _, %{app_context: app_context}) do
    Config.publish(endpoint, "EchoSettingsUpdated", %{reverse: true})
    {:ok, app_context}
  end
end
```

Let's try! Start the configuration server first.

```
iex(1)> Eventail.start_link(
...(1)>            hosts: ["amqp://localhost"],
...(1)>            logical_service: :configurator,
...(1)>            name: :config_service,
...(1)>            setup: [
...(1)>              configurations: ["EchoSettingsRequested", "ConfiguratorStarted"],
...(1)>              handlers: Configurator
...(1)>            ]
...(1)> )
{:ok, #PID<0.338.0>}
iex(2)>
15:38:39.696 [info]  Connected to broker!

15:38:39.712 [info]  Expecting configuration events ["EchoSettingsRequested", "ConfiguratorStarted"]

nil
iex(3)> Eventail.start_link(
...(3)>            hosts: ["amqp://localhost"],
...(3)>            logical_service: :echo,
...(3)>            name: :echo_service,
...(3)>            setup: [
...(3)>              commands: ["echo.EchoMessage"],
...(3)>              configurations: ["EchoSettingsUpdated"],
...(3)>              handlers: Echo
...(3)>            ]
...(3)> )
{:ok, #PID<0.375.0>}
iex(4)>
15:39:12.257 [info]  Connected to broker!

15:39:12.273 [info]  Expecting configuration events ["EchoSettingsUpdated"]

15:39:12.293 [debug] Application %Eventail.Endpoint{hostname: "rtxmpro", logical_service: :echo, pid: #PID<0.375.0>}
logged on EDA: %{:full_message => "reverse = true", :host => "echo@rtxmpro", :level => 6,
:short_message => "Echo configured", :timestamp => 1654090752.28, :version => "1.1", "_conversation_id" => "conf", "_levelname" => :INFO, "_logical_service" => :echo, "_worker_pid" => "#PID<0.375.0>"}

15:39:12.301 [info]  Service echo subscribed to commands/results ["echo.EchoMessage"]

15:39:12.304 [debug] Application %Eventail.Endpoint{hostname: "rtxmpro", logical_service: :echo, pid: #PID<0.375.0>}
logged on EDA: %{:full_message => "I'm alive!", :host => "echo@rtxmpro", :level => 5, :short_message => "Echo health check", :timestamp => 1654090752.304, :version => "1.1", "_another_custom_gelf_field" => 123, "_conversation_id" => "7aece4b9-6a17-42f7-b166-5ad359df093e", "_custom_gelf" => "for the fun", "_levelname" => :NOTICE, "_logical_service" => :echo, "_worker_pid" => "#PID<0.400.0>"}

nil
iex(5)> Eventail.start_link(
...(5)>   hosts: ["amqp://localhost"],
...(5)>   logical_service: :ping,
...(5)>   name: :ping_service,
...(5)>   setup: [
...(5)>     commands: ["ping.Result"],
...(5)>     handlers: Ping
...(5)>   ])
{:ok, #PID<0.410.0>}
iex(6)>
15:39:57.406 [info]  Connected to broker!

15:39:57.414 [info]  Expecting configuration events []

15:39:57.419 [info]  Service ping subscribed to commands/results ["ping.Result"]

nil
iex(7)> ping_endpoint = Eventail.get_endpoint(:ping_service)
%Eventail.Endpoint{
  hostname: "rtxmpro",
  logical_service: :ping,
  pid: #PID<0.410.0>
}
iex(8)> Eventail.Business.send_command(
...(8)>   ping_endpoint, "echo.EchoMessage", %{message: "hello world!"}, UUID.uuid4(), "ping.Result", "corr1")
:ok
iex(9)>
15:40:24.055 [debug] Application %Eventail.Endpoint{hostname: "rtxmpro", logical_service: :echo, pid: #PID<0.375.0>}
logged on EDA: %{:full_message => "Message to echo = hello world!", :host => "echo@rtxmpro", :level => 6, :short_message => "Received Echo command", :timestamp => 1654090824.055, :version => "1.1", "_conversation_id" => "203e68bf-4aab-4a87-9ba4-3f3ec5276ff5", "_levelname" => :INFO, "_logical_service" => :echo, "_worker_pid" => "#PID<0.375.0>"}

15:40:24.058 [debug] Application %Eventail.Endpoint{hostname: "rtxmpro", logical_service: :ping, pid: #PID<0.410.0>}
logged on EDA: %{:full_message => "Echo = !dlrow olleh", :host => "ping@rtxmpro", :level => 6, :short_message => "Received echo", :timestamp => 1654090824.057, :version => "1.1", "_conversation_id" => "203e68bf-4aab-4a87-9ba4-3f3ec5276ff5", "_correlation_id" => "corr1", "_levelname" => :INFO, "_logical_service" => :ping, "_worker_pid" => "#PID<0.410.0>"}

nil
```
