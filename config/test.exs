import Config

# rocksdb is disabled for testing because it slows tests down too much
config :anoma_node, :mnesia,
  persist_to_disk: true,
  rocksdb: false
