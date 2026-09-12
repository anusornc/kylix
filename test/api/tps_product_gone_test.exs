defmodule Kylix.API.TpsProductGoneTest do
  use ExUnit.Case, async: true

  test "TPS run module is not loaded" do
    refute Code.ensure_loaded?(Kylix.Benchmark.TransactionSpeed)
  end

  test "TPS visualizer is not loaded" do
    refute Code.ensure_loaded?(Kylix.Benchmark.ResultVisualizer)
  end

  test "dashboard is not a TPS page" do
    html = Kylix.API.Dashboard.render()

    refute html =~ "Performance Benchmarks"
    refute html =~ "Tx/sec"
    refute html =~ "Cache Performance"
    refute html =~ "/run-benchmark"

    assert html =~ "Submit Transaction"
    assert html =~ "Run Query"

    refute html =~ "SPARQL"
    refute html =~ "SPARQL query"
    refute html =~ "blockchain explorer"
    refute html =~ "valid_sig"
  end

  test "AuthFlow HTML is gone" do
    refute File.exists?(Path.expand("lib/frontend/index.html"))
  end
end
