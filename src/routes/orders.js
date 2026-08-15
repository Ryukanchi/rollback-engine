const express = require("express");

const { createHttpError } = require("../middleware/errorHandler");
const {
  readCommandContext,
  setCommandResponseHeaders,
} = require("./commandContext");

function assertRequestBody(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw createHttpError(400, "JSON request body is required");
  }
}

function parseAggregateId(value) {
  if (typeof value !== "string" || !/^\d+$/.test(value)) {
    throw createHttpError(400, "orderId must be a positive safe integer");
  }

  const aggregateId = Number(value);

  if (!Number.isSafeInteger(aggregateId) || aggregateId <= 0) {
    throw createHttpError(400, "orderId must be a positive safe integer");
  }

  return aggregateId;
}

function createOrdersRouter({ rollbackEngine }) {
  const router = express.Router();

  router.get("/orders", (req, res, next) => {
    try {
      return res.json(rollbackEngine.listOrders());
    } catch (error) {
      return next(error);
    }
  });

  router.post("/order", (req, res, next) => {
    try {
      assertRequestBody(req.body);

      const { item, quantity = 1 } = req.body;

      if (typeof item !== "string" || item.trim().length === 0) {
        throw createHttpError(400, "item must be a non-empty string");
      }

      if (!Number.isSafeInteger(quantity) || quantity <= 0) {
        throw createHttpError(400, "quantity must be a positive safe integer");
      }

      const commandContext = readCommandContext(req);
      setCommandResponseHeaders(res, commandContext);
      const result = rollbackEngine.createOrder(
        { item, quantity },
        commandContext
      );

      return res.status(201).json(result.state.order);
    } catch (error) {
      return next(error);
    }
  });

  router.delete("/order/:id", (req, res, next) => {
    try {
      const aggregateId = parseAggregateId(req.params.id);
      const commandContext = readCommandContext(req);
      setCommandResponseHeaders(res, commandContext);
      const result = rollbackEngine.deleteOrder(
        aggregateId,
        "Order deleted",
        commandContext
      );

      return res.json({
        message: "Order deleted",
        order: result.deletedOrder,
        warnings: result.warnings,
      });
    } catch (error) {
      return next(error);
    }
  });

  return router;
}

module.exports = {
  createOrdersRouter,
};
