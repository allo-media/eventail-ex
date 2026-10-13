defmodule Eventail.MixProject do
  use Mix.Project

  @source_url "https://github.com/allo-media/eventail-ex"

  def project do
    [
      app: :eventail,
      version: "2.1.2",
      elixir: "~> 1.17",
      test_coverage: [summary: [threshold: 80]],
      start_permanent: Mix.env() == :prod,
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        plt_add_apps: [:ex_unit]
      ],
      deps: deps(),
      description:
        "Idiomatic Elixir library to build services on an AMQP based Event Driven Architecture.",
      package: package(),
      source_url: @source_url,
      docs: [
        main: "readme",
        extras: ["README.md"]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE .formatter.exs)
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:amqp, "~> 4.1.0"},
      {:amqp_client, "~> 4.2"},
      {:cbor, "~> 1.0"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:jason, "~> 1.4"}
    ]
  end
end
