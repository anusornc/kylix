defmodule Mix.Tasks.Eval.Kylix do
  use Mix.Task

  @shortdoc "Load Fig1 and emit the lineage correctness table"

  @moduledoc """
  Records the Fig1 pipeline through the public record API, runs the lineage
  suite, diffs `eval/expected/`, and prints the paper correctness table.

  Fabric on-chain is reported as not answerable. No latency is published.

  This task generates a key pair, seeds that Validator, and records Fig1
  through `Kylix.add_transaction/5`. Drive it with `eval/run_kylix.sh`
  (`MIX_ENV=test`).
  """

  @impl Mix.Task
  def run(_args) do
    unless Mix.env() == :test do
      Mix.raise(
        "eval.kylix records Fig1 in MIX_ENV=test; run MIX_ENV=test mix eval.kylix (see eval/run_kylix.sh)"
      )
    end

    Mix.Task.run("app.start")
    Kylix.Storage.DAGEngine.clear_all()
    :ok = Kylix.BlockchainServer.reset_tx_count(0)

    {:ok, {public_key, private_key}} = Kylix.Auth.SignatureVerifier.generate_test_key_pair()
    {:ok, attester} = Kylix.add_validator("fig1_attester", public_key, "agent1")

    result =
      case Kylix.Eval.Runner.run(validator_id: attester, private_key: private_key) do
        {:ok, _rows} -> :ok
        {:error, _rows} -> Mix.raise("Lineage suite did not match expected bindings")
      end

    File.rm("config/validators/fig1_attester.pub")
    result
  end
end
