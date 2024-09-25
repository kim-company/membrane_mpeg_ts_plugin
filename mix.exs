defmodule Membrane.MPEG.TS.MixProject do
  use Mix.Project

  def project do
    [
      app: :membrane_mpeg_ts_plugin,
      version: "0.1.0",
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
      {:membrane_core, "~> 1.0"},
      {:membrane_h264_format, "~> 0.6.1"},
      {:kim_mpeg_ts, "~> 1.0"},
      {:membrane_file_plugin, ">= 0.0.0", only: :test},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]
end
