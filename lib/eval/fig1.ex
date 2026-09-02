defmodule Kylix.Eval.Fig1 do
  @moduledoc """
  Loads the Fig1 pipeline as signed Transactions through the public record API.
  """

  alias Kylix.Auth.SignatureVerifier

  @fixture_path Path.join(["eval", "fig1-transactions.json"])

  @doc """
  Reads the unsigned Fig1 fixture, signs each fact at load time, and records
  it with `Kylix.add_transaction/5`.

  Returns `{:ok, count}` or `{:error, reason}`.
  """
  def record(opts) when is_list(opts) do
    validator_id = Keyword.fetch!(opts, :validator_id)
    private_key = Keyword.fetch!(opts, :private_key)

    with {:ok, body} <- File.read(fixture_path()),
         {:ok, facts} <- Jason.decode(body) do
      record_facts(facts, validator_id, private_key)
    end
  end

  defp record_facts(facts, validator_id, private_key) do
    Enum.reduce_while(facts, {:ok, 0}, fn fact, {:ok, n} ->
      s = Map.fetch!(fact, "s")
      p = Map.fetch!(fact, "p")
      o = Map.fetch!(fact, "o")
      timestamp = DateTime.utc_now()
      tx_hash = SignatureVerifier.hash_transaction(s, p, o, validator_id, timestamp)
      signature = SignatureVerifier.sign(tx_hash, private_key)

      case Kylix.add_transaction(s, p, o, validator_id, signature) do
        {:ok, _tx_id} -> {:cont, {:ok, n + 1}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp fixture_path do
    Path.expand(@fixture_path)
  end
end
