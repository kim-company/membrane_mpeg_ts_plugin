defmodule Membrane.MPEG.TS.MixProject do
  use Mix.Project

  def project do
    [
      app: :membrane_mpeg_ts_plugin,
      version: "2.0.2",
      elixir: "~> 1.13",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      source_url: "https://github.com/kim-company/membrane_mpeg_ts_plugin",
      name: "Membrane MPEG.TS Plugin",
      description: description(),
      package: package(),
      deps: deps()
    ]
  end

  defp description do
    """
    MPEG-TS Demuxer that implements the Membrane.Filter behaviour.
    """
  end

  defp package do
    [
      maintainers: ["KIM Keep In Mind"],
      files: ~w(lib mix.exs README.md LICENSE),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/kim-company/membrane_mpeg_ts_plugin"}
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
      # {:mpeg_ts, "~> 3.0"},
      {:mpeg_ts, path: "../mpeg_ts"},
      {:membrane_core, "~> 1.0"},
      {:membrane_timestamp_queue, "~> 0.2.2"},
      {:membrane_file_plugin, ">= 0.0.0", only: :test},
      {:membrane_nalu_plugin, "~> 0.1", only: :test},
      {:membrane_aac_plugin, "~> 0.19.1", only: :test},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]
end
