defmodule Mix.Tasks.Eval.KylixTest do
  use ExUnit.Case
  import ExUnit.CaptureIO

  setup do
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)
    :ok
  end

  test "mix eval.kylix emits the correctness table" do
    parent = self()

    output =
      capture_io(fn ->
        send(parent, {:run, Mix.Tasks.Eval.Kylix.run([])})
      end)

    assert_received {:run, :ok}
    assert output =~ "not answerable"
    assert output =~ "activity:plot"
    assert output =~ "Kylix"
  end
end
