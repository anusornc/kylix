defmodule Mix.Tasks.Eval.Kylix do
  use Mix.Task

  @shortdoc "Load Fig1 and emit the lineage correctness table"

  @moduledoc """
  Records the Fig1 pipeline through the public record API, runs the lineage
  suite, diffs `eval/expected/`, and prints the paper correctness table.

  Fabric on-chain is reported as not answerable. No latency is published.

  This task uses the same test-key path as the Fig1 ExUnit setup. Drive it
  with `eval/run_kylix.sh` (`MIX_ENV=test`).
  """

  @impl Mix.Task
  def run(_args) do
    unless Mix.env() == :test do
      Mix.raise(
        "eval.kylix uses the test-key path; run MIX_ENV=test mix eval.kylix (see eval/run_kylix.sh)"
      )
    end

    Mix.Task.run("app.start")
    Kylix.Storage.DAGEngine.clear_all()
    :ok = Kylix.BlockchainServer.reset_tx_count(0)

    {:ok, %{private_key: private_key}} =
      GenServer.call(Kylix.BlockchainServer, :get_test_key_pair)

    case Kylix.Eval.Runner.run(validator_id: "agent1", private_key: private_key) do
      {:ok, _rows} -> :ok
      {:error, _rows} -> Mix.raise("Lineage suite did not match expected bindings")
    end
  end
end
