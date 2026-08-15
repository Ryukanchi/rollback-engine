const express = require("express");

const { createHttpError } = require("../middleware/errorHandler");

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

function parseSequence(value) {
  if (typeof value !== "string" || !/^\d+$/.test(value)) {
    throw createHttpError(400, "sequence must be a non-negative safe integer");
  }

  const sequence = Number(value);

  if (!Number.isSafeInteger(sequence) || sequence < 0) {
    throw createHttpError(400, "sequence must be a non-negative safe integer");
  }

  return sequence;
}

function parseTimestamp(value) {
  if (typeof value !== "string" || value.length === 0) {
    throw createHttpError(400, "timestamp must be a valid date string");
  }

  const timestamp = new Date(value);

  if (Number.isNaN(timestamp.getTime())) {
    throw createHttpError(400, "timestamp must be a valid date string");
  }

  return timestamp.toISOString();
}

function requireState(state, aggregateId) {
  if (!state) {
    throw createHttpError(
      404,
      `Aggregate ${aggregateId} does not exist`,
      "AGGREGATE_NOT_FOUND"
    );
  }

  return state;
}

function createReplayRouter({ rollbackEngine }) {
  const router = express.Router();

  router.get("/history", (req, res, next) => {
    try {
      const history = rollbackEngine.getAllEvents();

      return res.json({
        count: history.length,
        history,
      });
    } catch (error) {
      return next(error);
    }
  });

  router.get("/diagnostics", (req, res, next) => {
    try {
      const { type, status, aggregateId, commandId, limit } = req.query;
      const parsedLimit = limit ? Number(limit) : undefined;

      const diagnostics = rollbackEngine.getDiagnostics({
        type: type || undefined,
        status: status || undefined,
        aggregateId: aggregateId || undefined,
        commandId: commandId || undefined,
        limit: Number.isSafeInteger(parsedLimit) ? parsedLimit : undefined,
      });

      return res.json({
        count: diagnostics.length,
        diagnostics,
      });
    } catch (error) {
      return next(error);
    }
  });

  router.get("/timeline/:orderId", (req, res, next) => {
    try {
      const aggregateId = parseAggregateId(req.params.orderId);
      const timeline = requireState(
        rollbackEngine.getTimeline(aggregateId),
        aggregateId
      );

      return res.json(timeline);
    } catch (error) {
      return next(error);
    }
  });

  router.get("/replay-state/:orderId", (req, res, next) => {
    try {
      const aggregateId = parseAggregateId(req.params.orderId);
      const state = requireState(rollbackEngine.replay(aggregateId), aggregateId);

      return res.json(state);
    } catch (error) {
      return next(error);
    }
  });

  router.get("/snapshot/:orderId", (req, res, next) => {
    try {
      const aggregateId = parseAggregateId(req.params.orderId);
      const snapshot = rollbackEngine.getSnapshot(aggregateId);

      if (!snapshot) {
        throw createHttpError(404, `Snapshot for aggregate ${aggregateId} does not exist`);
      }

      return res.json({ snapshot });
    } catch (error) {
      return next(error);
    }
  });

  router.post("/replay-restore/:orderId", (req, res, next) => {
    try {
      const aggregateId = parseAggregateId(req.params.orderId);
      const state = requireState(rollbackEngine.recover(aggregateId), aggregateId);

      return res.json({
        orderId: aggregateId,
        restored: true,
        state,
      });
    } catch (error) {
      return next(error);
    }
  });

  router.get("/state-at/:orderId/:timestamp", (req, res, next) => {
    try {
      const aggregateId = parseAggregateId(req.params.orderId);
      const timestamp = parseTimestamp(req.params.timestamp);
      const state = requireState(
        rollbackEngine.replayAt(aggregateId, timestamp),
        aggregateId
      );

      return res.json(state);
    } catch (error) {
      return next(error);
    }
  });

  router.get("/state-at/:orderId/sequence/:sequence", (req, res, next) => {
    try {
      const aggregateId = parseAggregateId(req.params.orderId);
      const sequence = parseSequence(req.params.sequence);
      const state = requireState(
        rollbackEngine.replayAtSequence(aggregateId, sequence),
        aggregateId
      );

      return res.json(state);
    } catch (error) {
      return next(error);
    }
  });

  return router;
}

module.exports = {
  createReplayRouter,
};
