defmodule Kylix.Eval.RunnerTest do
  use ExUnit.Case
  import ExUnit.CaptureIO

  setup do
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)
    Kylix.Storage.DAGEngine.clear_all()
    :ok = Kylix.BlockchainServer.reset_tx_count(0)

    {:ok, %{private_key: private_key}} =
      GenServer.call(Kylix.BlockchainServer, :get_test_key_pair)

    {:ok, private_key: private_key}
  end

  test "loads Fig1, executes lineage queries, and diffs expected JSON", %{
    private_key: private_key
  } do
    {result, _output} = run_eval(private_key)
    assert {:ok, rows} = result

    assert Enum.map(rows, & &1.name) == [
             "generated-by",
             "used",
             "derived-from",
             "derived-from-chain",
             "attributed-to",
             "activity-outputs",
             "count"
           ]

    assert Enum.all?(rows, & &1.match?)
  end

  test "emits the paper correctness table with Fabric on-chain not answerable", %{
    private_key: private_key
  } do
    {result, output} = run_eval(private_key)
    assert {:ok, _rows} = result

    assert output =~ "Kylix"
    assert output =~ "Fabric"
    assert output =~ "not answerable"
    assert output =~ "activity:plot"
    assert output =~ "entity:model"
    assert output =~ "entity:cleaned-data"
    assert output =~ "entity:raw-measurements"
    assert output =~ "agent:alice"
    assert output =~ "entity:fig1"
    assert output =~ ~r/\b3\b/
    refute output =~ ~r/latency/i
    refute output =~ ~r/\bTPS\b/
  end

  defp run_eval(private_key) do
    parent = self()

    output =
      capture_io(fn ->
        send(
          parent,
          {:run, Kylix.Eval.Runner.run(validator_id: "agent1", private_key: private_key)}
        )
      end)

    assert_received {:run, result}
    {result, output}
  end
end
