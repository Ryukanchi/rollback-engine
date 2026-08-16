const ITEMS = [
  "QuantumSensor",
  "RoboticArm",
  "HighEndGPU",
  "ServerRack",
  "LaserEmitter",
  "DroneMotor",
  "PrecisionCalibrator",
  "OpticTransceiver",
];

const FAILURE_POINTS = [
  null,
  "after_order",
  "after_inventory",
  "after_payment",
  "invalid_failure_point",
];

const OPERATION_TYPES = [
  "CHECKOUT",
  "CREATE_ORDER",
  "DELETE_ORDER",
  "COMPENSATE",
  "RETRY_SAME_COMMAND",
  "RETRY_CONFLICTING_COMMAND",
  "AUTHORITATIVE_READ",
  "MATERIALIZED_READ",
  "RECOVER",
  "SNAPSHOT_CREATE",
  "REPLAY",
  "REPLAY_AT_SEQUENCE",
  "VIEW_CORRUPT",
  "VIEW_DELETE",
  "SNAPSHOT_CORRUPT",
  "SNAPSHOT_DELETE",
  "CONCURRENT_APPEND_ATTEMPT",
  "INTERRUPTED_COMMIT_SIMULATION",
  "PROCESSING_ZERO_SIMULATION",
  "SCHEMA_UPCAST_TEST",
];

class OperationGenerator {
  #random;

  constructor(seededRandom) {
    this.#random = seededRandom;
  }

  generateOperation(context) {
    const { knownAggregates = [], knownCommands = [], maxOperations = 10, supportsSnapshotDelete = false } = context;

    // Weight operation selection based on available state
    const candidates = ["CHECKOUT", "CREATE_ORDER"];

    if (knownAggregates.length > 0) {
      candidates.push(
        "AUTHORITATIVE_READ",
        "MATERIALIZED_READ",
        "RECOVER",
        "SNAPSHOT_CREATE",
        "REPLAY",
        "REPLAY_AT_SEQUENCE",
        "VIEW_CORRUPT",
        "VIEW_DELETE",
        "SNAPSHOT_CORRUPT",
        "COMPENSATE",
        "DELETE_ORDER",
        "CONCURRENT_APPEND_ATTEMPT"
      );

      if (supportsSnapshotDelete) {
        candidates.push("SNAPSHOT_DELETE");
      }
    }

    if (knownCommands.length > 0) {
      candidates.push("RETRY_SAME_COMMAND", "RETRY_CONFLICTING_COMMAND");
    }

    // Occasionally test boundaries & leases
    if (this.#random.nextBoolean(0.25)) {
      candidates.push(
        "INTERRUPTED_COMMIT_SIMULATION",
        "PROCESSING_ZERO_SIMULATION",
        "SCHEMA_UPCAST_TEST",
        "LEASE_TAKEOVER_SIMULATION",
        "ZOMBIE_FENCING_SIMULATION",
        "MISSING_TOKEN_SIMULATION",
        "UNRECORDED_EVENT_TAKEOVER_SIMULATION"
      );
    }

    const opType = this.#random.pick(candidates);
    return this.#buildOperation(opType, context);
  }

  #buildOperation(opType, context) {
    const { knownAggregates = [], knownCommands = [] } = context;
    const aggId = knownAggregates.length > 0 ? this.#random.pick(knownAggregates) : null;
    const existingCmd = knownCommands.length > 0 ? this.#random.pick(knownCommands) : null;

    switch (opType) {
      case "CHECKOUT": {
        const item = this.#random.pick(ITEMS);
        const quantity = this.#random.nextInt(1, 10);
        const amount = this.#random.nextInt(50, 5000);
        const simulateFailureAt = this.#random.pick(FAILURE_POINTS);
        const commandId = `cmd-chk-${this.#random.nextString(8)}`;
        return {
          type: "CHECKOUT",
          params: {
            payload: { item, quantity, amount, simulateFailureAt },
            options: { commandId },
          },
        };
      }

      case "CREATE_ORDER": {
        const item = this.#random.pick(ITEMS);
        const quantity = this.#random.nextInt(1, 10);
        const commandId = `cmd-ord-${this.#random.nextString(8)}`;
        return {
          type: "CREATE_ORDER",
          params: {
            command: { item, quantity },
            options: { commandId },
          },
        };
      }

      case "DELETE_ORDER": {
        return {
          type: "DELETE_ORDER",
          params: {
            aggregateId: aggId ?? 1,
            reason: `Chaos delete reason ${this.#random.nextString(4)}`,
            options: { commandId: `cmd-del-${this.#random.nextString(8)}` },
          },
        };
      }

      case "COMPENSATE": {
        return {
          type: "COMPENSATE",
          params: {
            aggregateId: aggId ?? 1,
            reason: `Chaos compensation reason ${this.#random.nextString(4)}`,
            options: { commandId: `cmd-cmp-${this.#random.nextString(8)}` },
          },
        };
      }

      case "RETRY_SAME_COMMAND": {
        if (!existingCmd) return this.#buildOperation("CHECKOUT", context);
        return {
          type: "RETRY_SAME_COMMAND",
          params: {
            commandId: existingCmd.commandId,
            commandType: existingCmd.commandType,
            payload: existingCmd.payload,
          },
        };
      }

      case "RETRY_CONFLICTING_COMMAND": {
        if (!existingCmd) return this.#buildOperation("CHECKOUT", context);
        const modifiedPayload = { ...existingCmd.payload, quantity: (existingCmd.payload.quantity || 1) + 99 };
        return {
          type: "RETRY_CONFLICTING_COMMAND",
          params: {
            commandId: existingCmd.commandId,
            commandType: existingCmd.commandType,
            payload: modifiedPayload,
          },
        };
      }

      case "AUTHORITATIVE_READ":
        return { type: "AUTHORITATIVE_READ", params: { aggregateId: aggId ?? 1 } };

      case "MATERIALIZED_READ":
        return { type: "MATERIALIZED_READ", params: { aggregateId: aggId ?? 1 } };

      case "RECOVER":
        return { type: "RECOVER", params: { aggregateId: aggId ?? 1 } };

      case "SNAPSHOT_CREATE":
        return { type: "SNAPSHOT_CREATE", params: { aggregateId: aggId ?? 1 } };

      case "REPLAY":
        return { type: "REPLAY", params: { aggregateId: aggId ?? 1 } };

      case "REPLAY_AT_SEQUENCE":
        return {
          type: "REPLAY_AT_SEQUENCE",
          params: {
            aggregateId: aggId ?? 1,
            targetSequence: this.#random.nextInt(0, 10),
          },
        };

      case "VIEW_CORRUPT":
        return { type: "VIEW_CORRUPT", params: { aggregateId: aggId ?? 1 } };

      case "VIEW_DELETE":
        return { type: "VIEW_DELETE", params: { aggregateId: aggId ?? 1 } };

      case "SNAPSHOT_CORRUPT":
        return { type: "SNAPSHOT_CORRUPT", params: { aggregateId: aggId ?? 1 } };

      case "SNAPSHOT_DELETE":
        return { type: "SNAPSHOT_DELETE", params: { aggregateId: aggId ?? 1 } };

      case "CONCURRENT_APPEND_ATTEMPT":
        return {
          type: "CONCURRENT_APPEND_ATTEMPT",
          params: {
            aggregateId: aggId ?? 1,
            staleExpectedVersion: 0,
          },
        };

      case "INTERRUPTED_COMMIT_SIMULATION":
        return {
          type: "INTERRUPTED_COMMIT_SIMULATION",
          params: {
            commandId: `cmd-lostack-${this.#random.nextString(8)}`,
          },
        };

      case "PROCESSING_ZERO_SIMULATION":
        return {
          type: "PROCESSING_ZERO_SIMULATION",
          params: {
            commandId: `cmd-proc0-${this.#random.nextString(8)}`,
          },
        };

      case "SCHEMA_UPCAST_TEST":
        return {
          type: "SCHEMA_UPCAST_TEST",
          params: {
            item: this.#random.pick(ITEMS),
          },
        };

      case "LEASE_TAKEOVER_SIMULATION":
        return {
          type: "LEASE_TAKEOVER_SIMULATION",
          params: {
            commandId: `cmd-lease-${this.#random.nextInt(1000, 99999)}`,
          },
        };

      case "ZOMBIE_FENCING_SIMULATION":
        return {
          type: "ZOMBIE_FENCING_SIMULATION",
          params: {
            commandId: `cmd-fencing-${this.#random.nextInt(1000, 99999)}`,
          },
        };

      case "MISSING_TOKEN_SIMULATION":
        return {
          type: "MISSING_TOKEN_SIMULATION",
          params: {
            commandId: `cmd-missing-token-${this.#random.nextInt(1000, 99999)}`,
          },
        };

      case "UNRECORDED_EVENT_TAKEOVER_SIMULATION":
        return {
          type: "UNRECORDED_EVENT_TAKEOVER_SIMULATION",
          params: {
            commandId: `cmd-unrecorded-takeover-${this.#random.nextInt(1000, 99999)}`,
          },
        };

      default:
        return this.#buildOperation("CHECKOUT", context);
    }
  }

  generateScenarioPlan(length = 6) {
    // Generate an array of operations for a complete scenario iteration
    const ops = [];
    const localContext = { knownAggregates: [], knownCommands: [] };

    for (let i = 0; i < length; i++) {
      const op = this.generateOperation(localContext);
      ops.push(op);
      if (op.type === "CHECKOUT" || op.type === "CREATE_ORDER") {
        if (localContext.knownAggregates.length < 5) {
          localContext.knownAggregates.push(localContext.knownAggregates.length + 1);
        }
        localContext.knownCommands.push({
          commandId: op.params.options.commandId,
          commandType: op.type,
          payload: op.params.payload || op.params.command,
        });
      }
    }

    return ops;
  }
}

module.exports = {
  OperationGenerator,
  OPERATION_TYPES,
};
