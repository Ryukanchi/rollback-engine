const express = require("express");

const { FAILURE_POINTS } = require("../domain/checkoutSaga");
const { createHttpError } = require("../middleware/errorHandler");
const {
  readCommandContext,
  setCommandResponseHeaders,
} = require("./commandContext");

const SUPPORTED_FAILURE_POINTS = new Set(Object.values(FAILURE_POINTS));

function assertRequestBody(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw createHttpError(400, "JSON request body is required");
  }
}

function validateCheckoutRequest({ item, quantity, amount, simulateFailureAt }) {
  if (typeof item !== "string" || item.trim().length === 0) {
    throw createHttpError(400, "item must be a non-empty string");
  }

  if (!Number.isSafeInteger(quantity) || quantity <= 0) {
    throw createHttpError(400, "quantity must be a positive safe integer");
  }

  if (typeof amount !== "number" || !Number.isFinite(amount) || amount <= 0) {
    throw createHttpError(400, "amount must be a positive finite number");
  }

  if (
    simulateFailureAt !== undefined &&
    simulateFailureAt !== null &&
    !SUPPORTED_FAILURE_POINTS.has(simulateFailureAt)
  ) {
    throw createHttpError(
      400,
      `simulateFailureAt must be one of: ${Array.from(SUPPORTED_FAILURE_POINTS).join(", ")}`
    );
  }
}

function createCheckoutRouter({ rollbackEngine }) {
  const router = express.Router();

  router.post("/checkout", (req, res, next) => {
    try {
      assertRequestBody(req.body);

      const {
        item,
        quantity = 1,
        amount = 100,
        simulateFailureAt,
      } = req.body;

      validateCheckoutRequest({ item, quantity, amount, simulateFailureAt });

      const commandContext = readCommandContext(req);
      setCommandResponseHeaders(res, commandContext);

      const result = rollbackEngine.checkout({
        item,
        quantity,
        amount,
        simulateFailureAt,
      }, commandContext);
      const statusCode = result.status === "completed" ? 201 : 500;

      return res.status(statusCode).json(result);
    } catch (error) {
      return next(error);
    }
  });

  return router;
}

module.exports = {
  createCheckoutRouter,
};
