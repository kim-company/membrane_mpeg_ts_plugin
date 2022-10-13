defmodule Membrane.MPEG.TS.MixProject do
  use Mix.Project

  def project do
    [
      app: :membrane_mpeg_ts_plugin,
      version: "0.1.0",
      elixir: "~> 1.13",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:qex, "~> 0.5"},
      {:membrane_core, "~> 0.10.0"},
      {:telemetry, "~> 1.1"},
      {:membrane_file_plugin, "~> 0.12.0", only: :test}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]
end
