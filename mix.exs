defmodule Batch.Mixfile do
  use Mix.Project

  @version "0.2.0"

  def project do
    [
      app: :batch,
      name: "Batch",
      version: @version,
      elixir: "~> 1.4",
      elixirc_paths: elixirc_paths(Mix.env),
      deps: deps(),
      package: package(),
      preferred_cli_env: [docs: :docs],
      description: description(),
      docs: docs(),
      test_coverage: [tool: Batch.Cover, ignore_modules: [Batch.Supervisor]]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_),     do: ["lib"]

  defp deps do
    [
      {:credo, "~> 0.8.10", only: :dev},
      {:ex_doc, "~> 0.18.1", only: :docs}
    ]
  end

  defp description do
    """
    Batches are processes meant to execute several functions asynchronously, collecting
    their return values or returning the error of the first failure (fail-fast).
    """
  end

  defp package do
    [
      maintainers: ["Fernando Tapia Rico"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/fertapric/batch"},
      files: ~w(mix.exs LICENSE README.md lib)
    ]
  end

  defp docs do
    [
      source_ref: "v#{@version}",
      main: "Batch",
      canonical: "http://hexdocs.pm/batch",
      source_url: "https://github.com/fertapric/batch"
    ]
  end
end
