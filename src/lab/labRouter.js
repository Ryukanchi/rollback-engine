const express = require("express");
const { join } = require("node:path");
const { runScenario, repairScenario, replayScenarioSequence, getScenarioResult } = require("./scenarioRunner");
const { scenarioStore } = require("./scenarioStore");

function createLabRouter() {
  const router = express.Router();

  // 1. API Endpoints (only available in LAB_MODE)
  router.post("/api/scenarios/run", (req, res, next) => {
    try {
      const { scenarioType, storageType = "sqlite", options = {} } = req.body || {};
      const result = runScenario({ scenarioType, storageType, options });
      res.status(200).json(result);
    } catch (err) {
      next(err);
    }
  });

  router.get("/api/scenarios/:id", (req, res, next) => {
    try {
      const result = getScenarioResult(req.params.id);
      if (!result) {
        return res.status(404).json({
          error: "NotFound",
          message: `Scenario session not found or expired: ${req.params.id}`,
        });
      }
      res.status(200).json(result);
    } catch (err) {
      next(err);
    }
  });

  router.post("/api/scenarios/:id/repair", (req, res, next) => {
    try {
      const result = repairScenario(req.params.id);
      res.status(200).json(result);
    } catch (err) {
      next(err);
    }
  });

  router.get("/api/scenarios/:id/state/:sequence", (req, res, next) => {
    try {
      const result = replayScenarioSequence(req.params.id, req.params.sequence);
      res.status(200).json(result);
    } catch (err) {
      next(err);
    }
  });

  router.delete("/api/scenarios/:id", (req, res, next) => {
    try {
      const closed = scenarioStore.closeSession(req.params.id);
      res.status(200).json({ scenarioId: req.params.id, closed });
    } catch (err) {
      next(err);
    }
  });

  router.get("/api/health", (req, res) => {
    res.status(200).json({
      status: "ok",
      labMode: true,
      activeSessions: scenarioStore.sessions.size,
      timestamp: new Date().toISOString(),
    });
  });

  // 2. Static UI Asset Serving
  const publicDir = join(__dirname, "../../public/lab");
  router.use(express.static(publicDir));

  // Serve index.html for GET /lab or GET /lab/
  router.get("/", (req, res) => {
    res.sendFile(join(publicDir, "index.html"));
  });

  return router;
}

module.exports = {
  createLabRouter,
};
