defmodule Kylix.EnvironmentTest do
  use ExUnit.Case

  # This module attribute is evaluated at compile time
  @is_test Mix.env() == :test

  test "confirm we're running in test environment using Mix.env()" do
    assert Mix.env() == :test
    IO.puts("SUCCESS: Mix.env() confirms we're in test environment")

    # Also print the database path for verification
    db_path = Application.get_env(:kylix, :db_path)
    IO.puts("Database path: #{db_path}")
  end

  test "confirm module attribute @is_test is correctly set" do
    # Print the value of @is_test
    IO.puts("@is_test value: #{@is_test}")

    # It should be true when compiled in test environment
    assert @is_test == true
    IO.puts("SUCCESS: @is_test confirms we're in test environment")
  end

  test "demonstrate how @is_test affects conditional code" do
    result = if @is_test do
      "test environment behavior"
    else
      "production environment behavior"
    end

    assert result == "test environment behavior"
    IO.puts("SUCCESS: Conditional code using @is_test works as expected")
  end
end
