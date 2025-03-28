ExUnit.start()
{:ok, _} = Application.ensure_all_started(:kylix)
# Import start_supervised/1 from Supervisor
import Supervisor
{:ok, _} = start_supervised(Kylix.BlockchainServer)
