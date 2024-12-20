defmodule Anoma.Node.Supervisor do
  @moduledoc """
  I am the top level supervisor for the Anoma node.
  """

  use Supervisor

  require Logger

  alias Anoma.Node.Tables
  @spec child_spec(any()) :: map()
  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary
    }
  end

  @spec start_link(
          list(
            {:node_id, String.t()}
            | {:grpc_port, non_neg_integer}
            | {:tx_args, any()}
          )
        ) :: term()
  def start_link(args) do
    args =
      Keyword.validate!(args, [
        :node_id,
        :grpc_port,
        :tx_args,
        try_replay: false
      ])

    name = Anoma.Node.Registry.via(args[:node_id], __MODULE__)
    Supervisor.start_link(__MODULE__, args, name: name)
  end

  @impl true
  def init(args) do
    Logger.debug("starting node with #{inspect(args)}")
    Process.set_label(__MODULE__)

    args =
      Keyword.validate!(args, [
        :node_id,
        :tx_args,
        grpc_port: 0,
        try_replay: true
      ])

    # initialize storage. if it fails, abort
    case initialize_storage(args[:node_id]) do
      {:ok, state} ->
        replay? = state == :existing_node and args[:try_replay]

        replay_args =
          if replay? do
            {:ok, replay_args} = attempt_replay(args[:node_id])
            replay_args
          else
            args[:tx_args]
          end

        children = [
          {Anoma.Node.Transport.Supervisor,
           node_id: args[:node_id], grpc_port: args[:grpc_port]},
          {Anoma.Node.Transaction.Supervisor,
           [node_id: args[:node_id], tx_args: replay_args]},
          {Anoma.Node.Intents.Supervisor, node_id: args[:node_id]},
          {Anoma.Node.Logging, node_id: args[:node_id]}
        ]

        Supervisor.init(children, strategy: :one_for_all)

      _err ->
        {:stop, :failed_to_init_storage}
    end
  end

  ############################################################
  #                  Private Helpers                         #
  ############################################################

  @spec initialize_storage(String.t()) ::
          {:ok, :existing_node | :new_node}
          | {:error, :failed_to_initialize_storage}
  defp initialize_storage(node_id) do
    # check if the node has existing tables, and initialize them if need be.
    case Tables.initialize_tables_for_node(node_id) do
      {:ok, :created} ->
        {:ok, :new_node}

      {:ok, :existing} ->
        {:ok, :existing_node}

      {:error, _e} ->
        {:error, :failed_to_initialize_storage}
    end
  end

  defp attempt_replay(node_id) do
    case Anoma.Node.Logging.try_replay(node_id) do
      {:ok, :replay_succeeded, replay_args} ->
        {:ok, replay_args}

      {:error, :replay_failed, replay_args} ->
        {:error, replay_args}
    end
  end
end
