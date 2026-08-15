const {
  InMemoryCommandStore,
} = require("../src/infrastructure/inMemoryCommandStore");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");
const {
  InMemorySnapshotStore,
} = require("../src/infrastructure/inMemorySnapshotStore");
const {
  InMemoryStateRepository,
} = require("../src/infrastructure/inMemoryStateRepository");
const {
  registerCommandStoreContract,
  registerEventStoreContract,
  registerSnapshotStoreContract,
  registerStateRepositoryContract,
} = require("./support/storeContractSuites");

registerEventStoreContract({
  adapterName: "InMemoryEventStore",
  createStore: () => new InMemoryEventStore(),
});

registerCommandStoreContract({
  adapterName: "InMemoryCommandStore",
  createStore: () => new InMemoryCommandStore(),
});

registerSnapshotStoreContract({
  adapterName: "InMemorySnapshotStore",
  createStore: () => new InMemorySnapshotStore(),
});

registerStateRepositoryContract({
  adapterName: "InMemoryStateRepository",
  createRepository: () => new InMemoryStateRepository(),
});
