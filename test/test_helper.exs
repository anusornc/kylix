ExUnit.start()
{:ok, _} = Application.ensure_all_started(:kylix)

defmodule Kylix.Test.App do
  @moduledoc false

  def restart do
    _ = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)
    :ok
  end
end

defmodule Kylix.Test.Attester do
  @moduledoc false

  alias Kylix.Auth.SignatureVerifier

  @pub_dir "config/validators"

  def generate_keys do
    {:ok, {public_key, private_key}} = SignatureVerifier.generate_test_key_pair()
    {public_key, private_key}
  end

  def seed(id, public_key, known_by \\ "integration_validator") do
    {:ok, ^id} = Kylix.add_validator(id, public_key, known_by)
    ExUnit.Callbacks.on_exit(fn -> File.rm(Path.join(@pub_dir, "#{id}.pub")) end)
    id
  end

  def signature(subject, predicate, object, validator_id, private_key) do
    subject
    |> SignatureVerifier.hash_transaction(predicate, object, validator_id)
    |> SignatureVerifier.sign(private_key)
  end
end
