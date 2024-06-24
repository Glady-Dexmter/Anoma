defmodule Anoma.Dump do
  @moduledoc """
  I provide an interface to dump current state and load appropriate
  external files to launch them as Anoma nodes.

  You can also use me to dump info such as current states and tables
  in a readable map format as well as get info stored in external
  files in binary format.

  ### Dumping API

  I give access to following public dumping functionality:

  - `dump/2`
  - `get_all/1`
  - `get_state/1`
  - `get_tables/1`

  ### Loading API

  I give access to following public loading functionality

  - `launch/2`
  - `launch/3`
  - `load/1`
  """

  alias Anoma.Configuration
  alias Anoma.Mnesia
  alias Anoma.Node

  alias Anoma.Node.{
    Logger,
    Pinger,
    Mempool,
    Executor,
    Clock,
    Storage,
    Router,
    Dumper
  }

  alias Anoma.Node.Ordering
  alias Anoma.Node.Router.Engine
  alias Anoma.Crypto.Id
  alias Anoma.System.Directories

  @doc """
  I dump the current state with storage. I accept a string as a name,
  so that the resulting file will be created as name.txt in the
  appropriate data directory. As a second argument I accept a node
  name whose info presented as a map I dump as a binary.

  Note that if the environment is `test` we do not use the XDG format
  for storing data and instead dump the files in the immadiate app
  folder.

  The map typing can be seen in `get_all`
  """

  @spec dump(Path.t(), atom()) :: {:ok, :ok} | {:error, any()}
  def dump(name, node) do
    dump_full_path(Directories.data(name), node)
  end

  def dump_full_path(name, node) do
    term = node |> get_all() |> :erlang.term_to_binary()

    name
    |> File.open([:write], fn file ->
      file |> IO.binwrite(term)
    end)
  end

  @doc """
  I launch a node given a file containing a binary version of an 12-tuple
  with appropriate info in the following order:
  - router id
  - mempool topic id
  - executor topic id
  - dumper
  - storage
  - logger
  - clock
  - ordering
  - mempool
  - pinger
  - executor
  - storage names
  - qualified
  - order
  - block_storage

  All engines have info on their states and id's so that checkpointing
  the system will keep all adresses used in the previous session.
  Note that I ensure that the apporpriate tables are new.

  Moreover, I ensure that the mempool and block storage are in sync.
  In particular, I check that the order of the last block is less than
  that of the mempool dumped. If not, I manually remove the last block.

  Check whether your transactions have had an assigned worker. If not,
  relaunch them directly. If blocks were out of sync with mempool,
  relaunch the executions as well.
  """

  @dialyzer {:no_return, launch: 2}
  @spec launch(String.t(), atom()) :: {:ok, %Node{}} | any()
  def launch(file, name) do
    load = file |> load()

    settings = block_check(load)

    node_settings = [
      new_storage: false,
      name: name,
      settings: settings,
      use_rocks: load[:use_rocks]
    ]

    Anoma.Node.start_link(node_settings)
  end

  @doc """
  I have the same functionality as `launch/2` but start the node using a
  named supervisor.
  """

  @dialyzer {:no_return, launch: 2}
  @spec launch(String.t(), atom(), atom(), Configuration.configuration_map()) ::
          {:ok, %Node{}} | any()
  def launch(file, name, sup_name, config) do
    load = file |> load()

    settings = block_check(load)

    node_settings = [
      new_storage: false,
      name: name,
      settings: settings,
      use_rocks: load[:use_rocks],
      configuration: config
    ]

    [{Anoma.Node, node_settings}]
    |> Supervisor.start_link(strategy: :one_for_one, name: sup_name)
  end

  @doc """
  I read the given file which I assume contains binary info and convert
  it to an Elixir term.

  As the dumped state may have extra atoms not present in the session,
  I currently allow for atom creation in the loaded term.
  """

  @spec load(String.t()) :: any() | dump()
  def load(name) do
    {:ok, bin} = File.read(name)
    Plug.Crypto.non_executable_binary_to_term(bin)
  end

  @doc """
  Removes the given dump files at the specified address and with the
  given configuration.

  See `Anoma.System.Directories` for more informaiton about the path
  resolution and for the second atom.
  """
  @spec remove_dump(Path.t()) :: :ok
  @spec remove_dump(Path.t(), atom()) :: :ok
  def remove_dump(file, env \\ Application.get_env(:anoma, :env)) do
    file |> Directories.data(env) |> File.rm!()
  end

  @type dump_eng :: {Id.Extern.t(), Dumper.t()}
  @type log_eng :: {Id.Extern.t(), Logger.t()}
  @type clock_eng :: {Id.Extern.t(), Clock.t()}
  @type ord_eng :: {Id.Extern.t(), Ordering.t()}
  @type mem_eng :: {Id.Extern.t(), Mempool.t()}
  @type ping_eng :: {Id.Extern.t(), Pinger.t()}
  @type ex_eng :: {Id.Extern.t(), Executor.t()}
  @type storage_eng :: {Id.Extern.t(), Storage.t()}
  @type configuration_eng :: {Id.Extern.t(), Anoma.Node.Configuration.t()}
  @type stores :: {Storage.t(), atom()}

  @type dump() :: %{
          router: Id.t(),
          transport: Id.t(),
          router_state: Router.t(),
          transport_id: Id.Extern.t(),
          logger_topic: Id.Extern.t(),
          mempool_topic: Id.Extern.t(),
          executor_topic: Id.Extern.t(),
          storage_topic: Id.Extern.t(),
          configuration: configuration_eng,
          logger: log_eng,
          clock: clock_eng,
          ordering: ord_eng,
          mempool: mem_eng,
          pinger: ping_eng,
          executor: ex_eng,
          storage: storage_eng,
          dumper: dump_eng,
          storage_data: stores,
          qualified: list(),
          order: list(),
          block_storage: list(),
          use_rocks: boolean()
        }

  @doc """
  I get all the info on the node tables and engines in order:
  - router
  - logger
  - clock
  - ordering
  - mempool
  - pinger
  - executor
  - table names
  - qualified
  - order
  - block_storage
  And turn the info into a tuple
  """

  @spec get_all(atom()) :: dump()
  def get_all(node) do
    Map.merge(get_state(node), get_tables(node))
  end

  @doc """
  I get the engine states in order:
  - router
  - mempool topic
  - executor topic
  - dumper
  - storage
  - logger
  - clock
  - ordering
  - mempool
  - pinger
  - executor
  """

  @spec get_state(atom()) ::
          %{
            router: Id.t(),
            transport: Id.t(),
            router_state: Router.t(),
            transport_id: Id.Extern.t(),
            logger_topic: Id.Extern.t(),
            mempool_topic: Id.Extern.t(),
            executor_topic: Id.Extern.t(),
            storage_topic: Id.Extern.t(),
            configuration: configuration_eng,
            logger: log_eng,
            clock: clock_eng,
            ordering: ord_eng,
            mempool: mem_eng,
            pinger: ping_eng,
            executor: ex_eng,
            storage: storage_eng,
            dumper: dump_eng
          }
  def get_state(node) do
    state = node |> Node.state()

    node =
      state
      |> Map.filter(fn {key, _value} ->
        key not in [
          :router,
          :transport,
          :logger_topic,
          :mempool_topic,
          :executor_topic,
          :storage_topic,
          :__struct__
        ]
      end)
      |> Map.to_list()

    list =
      node
      |> Enum.map(fn {atom, engine} ->
        %{atom => {engine.id, Engine.get_state(engine)}}
      end)

    map = Enum.reduce(list, fn x, acc -> Map.merge(acc, x) end)

    # EVIL, please make this not evil
    internal_transport_id =
      Engine.get_state(state.transport).transport_internal_id

    # This is rather bad, as we are peeking at the internal state, and
    # we are not using the engine, so it will have issues across
    # nodes....

    router_id =
      :sys.get_state(Process.whereis(state.router.server)).internal_id

    router_state = Anoma.Node.Router.dump_state(state.router.server)
    # Back to normal work

    Map.merge(
      %{
        router: router_id,
        router_state: router_state,
        transport: internal_transport_id,
        # public facing id for other nodes to talk to
        transport_id: state.transport.id,
        logger_topic: state.logger_topic.id,
        mempool_topic: state.mempool_topic.id,
        executor_topic: state.executor_topic.id,
        storage_topic: state.storage_topic.id
      },
      map
    )
  end

  @doc """
  I get the node tables in order:
  - storage (names)
  - qualified
  - order
  - block_storage
  """

  @spec get_tables(atom()) :: %{
          storage_data: stores,
          qualified: list(),
          order: list(),
          block_storage: list(),
          use_rocks: boolean()
        }
  def get_tables(node) do
    node = node |> Node.state()
    table = Engine.get_state(Engine.get_state(node.ordering).table)
    block = Engine.get_state(node.mempool).block_storage
    qual = table.qualified
    ord = table.order
    # TODO more robust checking here
    rocks =
      if :ram_copies == :mnesia.table_info(qual, :storage_type) do
        false
      else
        true
      end

    {q, o, b} =
      [qual, ord, block]
      |> Enum.map(fn x ->
        with {:ok, lst} <- Mnesia.dump(x) do
          Enum.map(lst, fn x -> hd(x) end)
        end
      end)
      |> List.to_tuple()

    %{
      storage_data: {table, block},
      qualified: q,
      order: o,
      block_storage: b,
      use_rocks: rocks
    }
  end

  defp block_check(map) do
    block_storage = map.block_storage

    if block_storage != [] do
      last_block_list = block_storage |> List.last()

      last_block = last_block_list |> Anoma.Block.decode()

      {_id, mempool} = map.mempool

      if last_block.round == mempool.round do
        Map.replace(
          map,
          :block_storage,
          List.delete(block_storage, last_block_list)
        )
      else
        map
      end
    else
      map
    end
  end
end