import type { Application } from '@framework/core';
import type { DomainEvent, EventBus } from '@framework/events';
import { defineJob, type JobDefinition, type QueueManager } from '@framework/queue';
import { WorkflowEvents } from './events';
import {
  WORKFLOW_ADVANCE_NAME,
  WORKFLOW_UNIT_NAME,
  workflowAdvancePayloadSchema,
  workflowUnitPayloadSchema,
} from './internal-jobs';
import type { WorkflowManager } from './manager';
import { type RunnerOptions, WorkflowRunner } from './runner';

export interface WorkflowEngineOptions extends RunnerOptions {
  /** Timeout of internal unit jobs (default 30 min — a unit may run a whole inline subtree). */
  readonly unitTimeoutMs?: number;
  /** How often expired waits are swept (default 30 seconds; 0 disables). */
  readonly sweepIntervalMs?: number;
}

/**
 * The worker-side workflow engine: the runner, its internal job definitions,
 * the wait-for-event bridge and the timeout sweep. It deliberately does NOT
 * own a queue consumer — merge `engine.jobs` into the application's regular
 * `JobWorker`, so one process consumes one queue and app jobs never collide
 * with workflow internals.
 */
export class WorkflowEngine {
  readonly runner: WorkflowRunner;
  /** Internal unit/advance job definitions; register alongside application jobs. */
  readonly jobs: readonly JobDefinition<{ runId: string; nodeId: string }, null>[];
  readonly #bus: EventBus | null;
  readonly #options: WorkflowEngineOptions;
  readonly #manager: WorkflowManager;
  #sweepTimer: ReturnType<typeof setInterval> | null = null;
  #subscriptions = new Map<string, () => void>();

  constructor(
    application: Application,
    manager: WorkflowManager,
    queue: QueueManager,
    bus: EventBus | null,
    options: WorkflowEngineOptions = {},
  ) {
    this.#manager = manager;
    this.#bus = bus;
    this.#options = options;
    const events = new WorkflowEvents(bus);
    this.runner = new WorkflowRunner(application, manager.store, manager, queue, events, {
      ...options,
      onWaitRegistered: (eventType) => void this.subscribeWait(eventType),
    });

    const unitJob = defineJob({
      name: WORKFLOW_UNIT_NAME,
      version: 1,
      payload: workflowUnitPayloadSchema,
      attempts: 3,
      backoffMs: 500,
      timeoutMs: options.unitTimeoutMs ?? 30 * 60_000,
      handle: async (payload) => {
        await this.runner.executeUnit(payload.runId, payload.nodeId);
        return null;
      },
    });
    const advanceJob = defineJob({
      name: WORKFLOW_ADVANCE_NAME,
      version: 1,
      payload: workflowAdvancePayloadSchema,
      attempts: 3,
      backoffMs: 500,
      timeoutMs: 60_000,
      handle: async (payload) => {
        await this.runner.advance(payload.runId, payload.nodeId);
        return null;
      },
    });
    this.jobs = [unitJob, advanceJob];
  }

  /** Starts the wait sweep and (re)subscribes parked wait stages. */
  start(): void {
    const sweepIntervalMs = this.#options.sweepIntervalMs ?? 30_000;
    if (sweepIntervalMs > 0) {
      this.#sweepTimer = setInterval(() => {
        this.runner.failExpiredWaiters().catch((error) => console.error('[workflow] wait sweep failed', error));
      }, sweepIntervalMs);
    }
    void this.#armWaitSubscriptions();
  }

  /** Stops the sweep and the bus subscriptions (the JobWorker drains itself). */
  async close(): Promise<void> {
    if (this.#sweepTimer) clearInterval(this.#sweepTimer);
    for (const unsubscribe of this.#subscriptions.values()) unsubscribe();
    this.#subscriptions.clear();
  }

  /** Channel of an event type: everything before its last dot. */
  static channelOf(eventType: string): string {
    const dot = eventType.lastIndexOf('.');
    return dot > 0 ? eventType.slice(0, dot) : eventType;
  }

  /** Subscribes the bridge to a wait stage's channel (idempotent per event type). */
  async subscribeWait(eventType: string): Promise<void> {
    if (!this.#bus || this.#subscriptions.has(eventType)) return;
    const channel = WorkflowEngine.channelOf(eventType);
    const handler = (event: DomainEvent): Promise<void> =>
      this.runner.onEvent(event).catch((error) => console.error('[workflow] wait bridge failed', error));
    const unsubscribe = await this.#bus.subscribe(channel, handler);
    this.#subscriptions.set(eventType, unsubscribe);
  }

  async #armWaitSubscriptions(): Promise<void> {
    if (!this.#bus) return;
    for (const stage of await this.#manager.store.waitingStages()) {
      if (stage.waitEvent) await this.subscribeWait(stage.waitEvent);
    }
  }
}
