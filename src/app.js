const express = require("express");
const openApiDocument = require("../openapi.json");

const { RollbackEngine } = require("./application/rollbackEngine");
const { createStorageAdapters } = require("./infrastructure/storageFactory");
const { createLabRouter } = require("./lab/labRouter");
const {
  errorHandler,
  notFoundHandler,
} = require("./middleware/errorHandler");
const { createCheckoutRouter } = require("./routes/checkout");
const { createOrdersRouter } = require("./routes/orders");
const { createReplayRouter } = require("./routes/replay");

function createDefaultEngine() {
  const storageType = process.env.STORAGE || "memory";
  const dbPath = process.env.DB_PATH || ":memory:";

  if (storageType === "sqlite") {
    const adapters = createStorageAdapters({ type: "sqlite", dbPath });
    return new RollbackEngine({
      eventStore: adapters.eventStore,
      commandStore: adapters.commandStore,
      snapshotStore: adapters.snapshotStore,
      stateRepository: adapters.stateRepository,
    });
  }

  return new RollbackEngine();
}

function createApp({ rollbackEngine, labMode } = {}) {
  const isLabMode = labMode ?? (process.env.LAB_MODE === "1" || process.env.LAB_MODE === "true");
  const effectiveEngine = rollbackEngine ?? createDefaultEngine();
  const app = express();

  app.use(express.json());

  app.get("/", (req, res) => {
    res.send("Rollback Engine läuft 😈");
  });

  app.get("/openapi.json", (req, res) => {
    res.json(openApiDocument);
  });

  if (isLabMode) {
    app.use("/lab", createLabRouter());
  }

  app.use(createOrdersRouter({ rollbackEngine: effectiveEngine }));
  app.use(createCheckoutRouter({ rollbackEngine: effectiveEngine }));
  app.use(createReplayRouter({ rollbackEngine: effectiveEngine }));

  app.use(notFoundHandler);
  app.use(errorHandler);

  return app;
}

const app = createApp();

module.exports = {
  app,
  createApp,
};
