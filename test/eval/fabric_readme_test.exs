defmodule Kylix.Eval.FabricReadmeTest do
  use ExUnit.Case

  @readme Path.expand("eval/fabric/README.md")
  @fixture Path.expand("eval/fig1-transactions.json")

  @keys [
    "fig1-tx-01",
    "fig1-tx-02",
    "fig1-tx-03",
    "fig1-tx-04",
    "fig1-tx-05",
    "fig1-tx-06",
    "fig1-tx-07",
    "fig1-tx-08",
    "fig1-tx-09",
    "fig1-tx-10",
    "fig1-tx-11",
    "fig1-tx-12",
    "fig1-tx-13",
    "fig1-tx-14",
    "fig1-tx-15"
  ]

  test "describes the Fabric opaque-payload baseline for the lineage suite" do
    assert File.exists?(@readme), "eval/fabric/README.md is the Fabric deliverable"
    text = File.read!(@readme)

    assert text =~ "one-channel test network"
    assert text =~ "PutState"
    assert text =~ "GetState"
    assert text =~ "opaque"

    refute text =~ "fig1-tx-16"

    assert text =~ ~r/not answerable/i
    assert text =~ "no CouchDB selectors or graph chaincode"
    assert text =~ "external index"
    assert text =~ ~r/not a Fabric query/i

    facts = @fixture |> File.read!() |> Jason.decode!()
    assert length(facts) == 15
    assert text =~ "eval/fig1-transactions.json"

    lines = String.split(text, "\n")

    Enum.zip(@keys, facts)
    |> Enum.each(fn {key, %{"s" => s, "p" => p, "o" => o}} ->
      row = Enum.find(lines, &String.contains?(&1, key))
      assert row, "missing #{key}"
      assert row =~ s
      assert row =~ p
      assert row =~ o
    end)

    refute text =~ ~r/latency/i
    refute text =~ ~r/\bTPS\b/
  end
end
