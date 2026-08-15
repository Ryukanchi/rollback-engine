const express = require("express");
const openApiDocument = require("../openapi.json");

const { RollbackEngine } = require("./application/rollbackEngine");
const {
  errorHandler,
  notFoundHandler,
} = require("./middleware/errorHandler");
const { createCheckoutRouter } = require("./routes/checkout");
const { createOrdersRouter } = require("./routes/orders");
const { createReplayRouter } = require("./routes/replay");

function createApp({ rollbackEngine = new RollbackEngine() } = {}) {
  const app = express();

  app.use(express.json());

  app.get("/", (req, res) => {
    res.send("Rollback Engine läuft 😈");
  });

  app.get("/openapi.json", (req, res) => {
    res.json(openApiDocument);
  });

  app.use(createOrdersRouter({ rollbackEngine }));
  app.use(createCheckoutRouter({ rollbackEngine }));
  app.use(createReplayRouter({ rollbackEngine }));

  app.use(notFoundHandler);
  app.use(errorHandler);

  return app;
}

const app = createApp();

module.exports = {
  app,
  createApp,
};
