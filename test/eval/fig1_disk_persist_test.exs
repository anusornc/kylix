defmodule Kylix.Eval.Fig1DiskPersistTest do
  use ExUnit.Case, async: false

  alias Kylix.Query.SparqlEngine

  setup do
    tmp = Path.join(System.tmp_dir!(), "kylix-fig1-disk-#{System.unique_integer([:positive])}")
    original_adapter = Application.get_env(:kylix, :persist_adapter)
    original_db_path = Application.get_env(:kylix, :db_path)

    on_exit(fn ->
      Application.put_env(:kylix, :persist_adapter, original_adapter)
      Application.put_env(:kylix, :db_path, original_db_path)
      Application.stop(:kylix)
      Application.ensure_all_started(:kylix)
      File.rm_rf!(tmp)
    end)

    :ok = Application.stop(:kylix)
    Application.put_env(:kylix, :persist_adapter, :disk)
    Application.put_env(:kylix, :db_path, tmp)
    {:ok, _} = Application.ensure_all_started(:kylix)

    {public_key, private_key} = Kylix.Test.Attester.generate_keys()
    attester = Kylix.Test.Attester.seed("fig1_disk_attester", public_key)
    assert {:ok, 15} = Kylix.Eval.Fig1.record(validator_id: attester, private_key: private_key)

    Kylix.Test.App.restart()
    :ok
  end

  for name <- Kylix.Eval.Suite.names() do
    test "#{name} matches expected bindings after Fig1 disk restart" do
      query = Kylix.Eval.Suite.query(unquote(name))
      expected = Kylix.Eval.Suite.expected(unquote(name))

      assert {:ok, results} = SparqlEngine.execute(query)
      assert results == expected
    end
  end
end
