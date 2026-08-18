const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { randomUUID } = require("node:crypto");
const { unlinkSync, existsSync } = require("node:fs");

const { createStorageAdapters } = require("../infrastructure/storageFactory");
const { RollbackEngine } = require("../application/rollbackEngine");

class ScenarioStore {
  constructor() {
    this.sessions = new Map();
  }

  createSession({ scenarioType = "custom", storageType = "sqlite" } = {}) {
    this.cleanupOldSessions();

    const scenarioId = randomUUID().slice(0, 8);
    const dbFileName = `rollback-lab-${scenarioId}.db`;
    const dbPath = join(tmpdir(), dbFileName);

    // Lease time is owned by the command store. A session that wants to
    // demonstrate expiry deterministically pins this value instead of handing a
    // chosen `now` to individual mutations; while it is null the store simply
    // runs on host wall-clock time.
    const leaseClock = { pinnedMs: null };
    const adapters = createStorageAdapters({
      type: storageType,
      dbPath: storageType === "sqlite" ? dbPath : ":memory:",
      leaseNow: () => leaseClock.pinnedMs ?? Date.now(),
      wal: true,
      busyTimeout: 5000,
    });

    const engine = new RollbackEngine({
      eventStore: adapters.eventStore,
      commandStore: adapters.commandStore,
      snapshotStore: adapters.snapshotStore,
      stateRepository: adapters.stateRepository,
    });

    const session = {
      scenarioId,
      scenarioType,
      storageType,
      dbFileName: storageType === "sqlite" ? dbFileName : "in-memory",
      dbPath,
      adapters,
      leaseClock,
      engine,
      createdAt: new Date().toISOString(),
      lastAccessedAt: Date.now(),
      stateCache: new Map(),
      extra: {},
    };

    this.sessions.set(scenarioId, session);
    return session;
  }

  getSession(scenarioId) {
    const session = this.sessions.get(scenarioId);
    if (!session) return null;
    session.lastAccessedAt = Date.now();
    return session;
  }

  closeSession(scenarioId) {
    const session = this.sessions.get(scenarioId);
    if (!session) return false;

    try {
      if (session.adapters && typeof session.adapters.close === "function") {
        session.adapters.close();
      }
    } catch {
      // Best-effort adapter close
    }

    if (session.storageType === "sqlite" && session.dbPath) {
      this.#deleteDbFiles(session.dbPath);
    }

    this.sessions.delete(scenarioId);
    return true;
  }

  cleanupOldSessions(maxAgeMs = 30 * 60 * 1000) {
    const now = Date.now();
    for (const [id, session] of this.sessions.entries()) {
      if (now - session.lastAccessedAt > maxAgeMs) {
        this.closeSession(id);
      }
    }
  }

  closeAll() {
    for (const id of Array.from(this.sessions.keys())) {
      this.closeSession(id);
    }
  }

  #deleteDbFiles(basePath) {
    const files = [basePath, `${basePath}-wal`, `${basePath}-shm`];
    for (const file of files) {
      try {
        if (existsSync(file)) {
          unlinkSync(file);
        }
      } catch {
        // Best-effort cleanup
      }
    }
  }
}

const scenarioStore = new ScenarioStore();

module.exports = {
  ScenarioStore,
  scenarioStore,
};
