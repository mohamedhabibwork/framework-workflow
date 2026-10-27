import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Application, ExecutionContext, type JsonObject } from '@framework/core';
import { createConnection, type DatabaseConnection } from '@framework/database';
import { MemoryEventBus } from '@framework/events';
import { createHttpKernel, forbidden, type MountAuthorization } from '@framework/http';
import type { JobWorker } from '@framework/queue';
import { createTestDatabase, requireTestDatabaseUrl, type TestDatabase } from '@framework/testing';
import { z } from 'zod/v4';
import type { RunRecord } from '../src';
import {
  defineAction,
  defineStage,
  defineWorkflow,
  parallel,
  WorkflowEngine,
  WorkflowManager,
  WorkflowStore,
  workflowRoutes,
} from '../src';

const databaseUrl = requireTestDatabaseUrl();
const redisUrl = process.env.REDIS_URL;
if (!redisUrl) throw new Error('REDIS_URL is not set; run `bun run db:up`');

const ACTOR = { id: 'tester', type: 'user' } as const;
const context = new ExecutionContext('test', 'corr-wf', ACTOR);

let database: TestDatabase;
let connection: DatabaseConnection;
let store: WorkflowStore;
let manager: WorkflowManager;
let engine: WorkflowEngine;
let worker: JobWorker;
let application: Application<object>;
const bus = new MemoryEventBus();
const compensationOrder: string[] = [];

const prepare = defineAction({
  name: 'prepare',
  run: async () => ({ prepared: true }),
});

const charge = defineAction({
  name: 'charge',
  input: z.object({ prepared: z.boolean() }),
  async run(input) {
    return { charged: input.prepared };
  },
});

const refund = defineAction({
  name: 'refund',
  run: async () => {
    compensationOrder.push('refund');
    return { refunded: true };
  },
  compensate: async () => {
    compensationOrder.push('compensate-refund');
  },
});

const alwaysFails = defineAction({
  name: 'always-fails',
  attempts: 1,
  run: async () => {
    throw new Error('boom');
  },
});

const queued = defineAction({
  name: 'queued',
  via: 'queue',
  run: async () => ({ queued: true }),
});

const guarded = defineAction({
  name: 'guarded',
  attempts: 1,
  precheck: async (state) => {
    if (!state.prepared) throw new Error('not prepared');
    return;
  },
  run: async () => ({ guarded: true }),
});

const shipping = defineStage({
  name: 'shipping',
  actions: parallel([queued, defineAction({ name: 'label', run: async () => ({ label: true }) })]),
});

const waitPayment = defineStage({
  name: 'wait-payment',
  waitFor: { event: 'payment.succeeded', timeoutMs: 10_000, pick: (event) => event.payload },
});

const happy = defineWorkflow({
  name: 'happy-order',
  version: 1,
  input: z.object({ orderId: z.string() }),
  stages: [defineStage({ name: 'prepare', actions: [prepare, charge, guarded] }), shipping],
});

const saga = defineWorkflow({
  name: 'saga-order',
  version: 1,
  input: z.object({ orderId: z.string() }),
  onFailure: 'compensate',
  stages: [defineStage({ name: 'pay', actions: [refund] }), defineStage({ name: 'fail', actions: [alwaysFails] })],
});

const halted = defineWorkflow({
  name: 'halted-order',
  version: 1,
  input: z.object({ orderId: z.string() }),
  onFailure: 'halt',
  stages: [defineStage({ name: 'broken', actions: [alwaysFails] })],
});

const waiter = defineWorkflow({
  name: 'waiter-order',
  version: 1,
  input: z.object({ orderId: z.string() }),
  stages: [defineStage({ name: 'prep', actions: [prepare] }), waitPayment],
});

let failFirstTime = true;
const onceBroken = defineAction({
  name: 'once-broken',
  attempts: 1,
  run: async () => {
    if (failFirstTime) throw new Error('first run fails');
    return { recovered: true };
  },
});

const retryable = defineWorkflow({
  name: 'retryable-order',
  version: 1,
  input: z.object({ orderId: z.string() }),
  onFailure: 'halt',
  stages: [defineStage({ name: 'broken-stage', actions: [onceBroken] })],
});

/** Run-state accessor: alias of `manager.status` for the polling loops below. */
const runState = (id: string) => manager.status(id);

async function settle(runId: string, timeoutMs = 15_000): Promise<RunRecord> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const status = await runState(runId);
    const state = status?.run.status;
    if (status && state && ['completed', 'failed', 'cancelled', 'compensated'].includes(state)) return status.run;
    await Bun.sleep(25);
  }
  throw new Error(`run ${runId} did not settle`);
}

beforeAll(async () => {
  database = await createTestDatabase({ adminUrl: databaseUrl });
  connection = createConnection({ url: database.url, maxConnections: 4 });
  await connection.sql`
    create table workflow_runs (
      id uuid primary key default gen_random_uuid(),
      workflow text not null,
      version integer not null,
      input jsonb not null,
      state jsonb not null default '{}',
      status text not null default 'pending',
      error text,
      actor jsonb,
      correlation_id text not null,
      created_at timestamptz not null default now(),
      started_at timestamptz,
      finished_at timestamptz
    )`;
  await connection.sql`
    create table workflow_nodes (
      id uuid primary key default gen_random_uuid(),
      run_id uuid not null references workflow_runs(id) on delete cascade,
      parent_id uuid references workflow_nodes(id) on delete cascade,
      kind text not null,
      key text not null,
      path text not null,
      node_index integer not null default 0,
      group_id text,
      via text not null default 'inline',
      status text not null default 'pending',
      attempts integer not null default 0,
      max_attempts integer not null default 1,
      output jsonb,
      error text,
      wait_event text,
      wait_deadline timestamptz,
      created_at timestamptz not null default now(),
      started_at timestamptz,
      finished_at timestamptz
    )`;

  store = new WorkflowStore(connection.db);
  application = new Application({ config: {}, providers: [] });
  const { JobWorker: jobWorkerCtor, QueueManager } = await import('@framework/queue');
  const queue = new QueueManager({ url: redisUrl, prefix: `wf-test-${crypto.randomUUID()}`, queue: 'wf-tests' });
  manager = new WorkflowManager(store, queue, bus);
  manager.register(happy, saga, halted, waiter, retryable);

  engine = new WorkflowEngine(application, manager, queue, bus, { sweepIntervalMs: 50 });
  worker = new jobWorkerCtor(queue, application, [...engine.jobs], { concurrency: 4 });
  await application.boot();
  worker.start();
  engine.start();
});

afterAll(async () => {
  await engine?.close();
  await worker?.close();
  await connection?.close();
  await database?.drop();
});

describe('Workflow engine (real Postgres + Redis)', () => {
  test('runs a full workflow: stages, actions, parallel groups, queued actions', async () => {
    const { runId } = await manager.dispatch(happy, { orderId: 'o-1' }, context);
    const run = await settle(runId);
    expect(run.status).toBe('completed');
    expect(run.state).toMatchObject({ prepared: true, charged: true, guarded: true, queued: true, label: true });

    const status = await runState(runId);
    const kinds = status?.nodes.map((n) => n.kind);
    expect(kinds).toContain('pipeline');
    expect(kinds).toContain('stage');
    expect(kinds).toContain('action');
    expect(status?.nodes.every((n) => ['completed', 'skipped'].includes(n.status))).toBe(true);
  });

  test('compensates completed actions newest-first when a later stage fails', async () => {
    compensationOrder.length = 0;
    const { runId } = await manager.dispatch(saga, { orderId: 'o-2' }, context);
    const run = await settle(runId);
    expect(run.status).toBe('compensated');
    expect(run.error).toContain('always-fails');
    // The refund action ran, then was compensated after the failure.
    expect(compensationOrder).toEqual(['refund', 'compensate-refund']);
    const status = await runState(runId);
    const refundNode = status?.nodes.find((n) => n.key === 'refund');
    expect(refundNode?.status).toBe('compensated');
    const failNode = status?.nodes.find((n) => n.key === 'always-fails');
    expect(failNode?.status).toBe('failed');
  });

  test('onFailure halt fails the run without compensating', async () => {
    compensationOrder.length = 0;
    const { runId } = await manager.dispatch(halted, { orderId: 'o-3' }, context);
    const run = await settle(runId);
    expect(run.status).toBe('failed');
    expect(compensationOrder).toEqual([]);
  });

  test('stage precheck failure fails the run with PRECHECK_FAILED', async () => {
    // 'gated' orders: break the invariant by dispatching a workflow whose first
    // stage is skipped so the precheck sees missing state.
    const brokenGated = defineWorkflow({
      name: `gated-skipped-${crypto.randomUUID().slice(0, 8)}`,
      version: 1,
      input: z.object({ orderId: z.string() }),
      onFailure: 'halt',
      stages: [
        defineStage({
          name: 'prep',
          actions: [defineAction({ name: 'nothing', when: () => false, run: async () => ({}) })],
        }),
        defineStage({
          name: 'gate',
          precheck: (state) => {
            if (!state.prepared) throw new Error('not prepared');
          },
          run: async () => ({ gated: true }),
        }),
      ],
    });
    manager.register(brokenGated);
    const { runId } = await manager.dispatch(brokenGated, { orderId: 'o-4' }, context);
    const run = await settle(runId);
    expect(run.status).toBe('failed');
    expect(run.error).toContain('PRECHECK_FAILED');
  });

  test('wait stages park the run and resume when the event arrives', async () => {
    const { runId } = await manager.dispatch(waiter, { orderId: 'o-5' }, context);
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      const status = await runState(runId);
      if (status?.run.status === 'waiting') break;
      await Bun.sleep(25);
    }
    const waiting = await runState(runId);
    expect(waiting?.run.status).toBe('waiting');

    await bus.publish({
      id: crypto.randomUUID(),
      type: 'payment.succeeded',
      channel: 'payment',
      payload: { paymentId: 'p-9' },
      correlationId: 'corr-wf',
      actor: ACTOR,
      occurredAt: new Date().toISOString(),
    });

    const run = await settle(runId);
    expect(run.status).toBe('completed');
    expect(run.state).toMatchObject({ paymentId: 'p-9', prepared: true });
  });

  test('cancelling a waiting run terminates it', async () => {
    const { runId } = await manager.dispatch(waiter, { orderId: 'o-6' }, context);
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      const status = await runState(runId);
      if (status?.run.status === 'waiting') break;
      await Bun.sleep(25);
    }
    expect(await manager.cancel(runId)).toBe(true);
    const run = await settle(runId);
    expect(run.status).toBe('cancelled');
  });

  test('retry resumes a failed run from the failure point', async () => {
    failFirstTime = true;
    const { runId } = await manager.dispatch(retryable, { orderId: 'o-7' }, context);
    const failedRun = await settle(runId);
    expect(failedRun.status).toBe('failed');

    failFirstTime = false;
    expect(await manager.retry(runId)).toBe(true);
    const run = await settle(runId);
    expect(run.status).toBe('completed');
    expect(run.state).toMatchObject({ recovered: true });
  });

  test('invalid input is rejected before a run is created', async () => {
    await expect(manager.dispatch(happy, { wrong: true } as unknown as JsonObject, context)).rejects.toThrow(
      /Invalid input/,
    );
  });

  test('unknown workflow names are rejected', async () => {
    await expect(manager.dispatch('nope', {}, context)).rejects.toThrow(/not registered/);
  });

  test('duplicate registration fails fast', async () => {
    expect(() => manager.register(happy)).toThrow(/registered twice/);
  });

  test('lists runs with pagination and filters', async () => {
    const page = await manager.list({ page: 1, limit: 10, workflow: 'happy-order', status: 'completed' });
    expect(page.meta.total).toBeGreaterThanOrEqual(1);
    expect(page.items.every((r) => r.workflow === 'happy-order')).toBe(true);
  });
});

describe('workflowRoutes mount-time authorization', () => {
  const stub = {
    list: async () => ({ items: [], meta: { page: 1, limit: 25, total: 0 } }),
    dispatch: async () => ({ runId: '00000000-0000-4000-8000-000000000000' }),
    status: async () => null,
    retry: async () => false,
    cancel: async () => false,
    resume: async () => false,
  } as unknown as WorkflowManager;
  const deny = () => {
    throw forbidden('denied by test guard');
  };
  const jsonPost = {
    method: 'POST',
    body: '{"workflow":"happy-order"}',
    headers: { 'content-type': 'application/json' },
  } as const;
  const plainPost = { method: 'POST' } as const;
  // The booted kernel application provides onError (validation → 422) and the
  // request scope the dispatch handler reads its execution context from.
  const kernel = (authorize: MountAuthorization) =>
    createHttpKernel(application, { exposeInternalErrors: false }).route(
      '/workflow',
      workflowRoutes({ manager: () => stub, authorize }),
    );

  test('a denying guard rejects every endpoint with 403 before validation', async () => {
    const routes = kernel(deny);
    expect((await routes.request('/workflow?limit=nope')).status).toBe(403);
    expect((await routes.request('/workflow/dispatch', jsonPost)).status).toBe(403);
    // An invalid run id would be 422 after validation; the guard runs first.
    expect((await routes.request('/workflow/not-a-uuid')).status).toBe(403);
    expect((await routes.request('/workflow/not-a-uuid/retry', jsonPost)).status).toBe(403);
    expect((await routes.request('/workflow/not-a-uuid/cancel', plainPost)).status).toBe(403);
    expect((await routes.request('/workflow/not-a-uuid/resume', plainPost)).status).toBe(403);
  });

  test('an allowing guard serves requests and translates unknown runs to 404', async () => {
    const routes = kernel(() => {});
    expect((await routes.request('/workflow')).status).toBe(200);
    expect((await routes.request('/workflow/dispatch', jsonPost)).status).toBe(202);
    expect((await routes.request('/workflow/00000000-0000-4000-8000-000000000000')).status).toBe(404);
  });
});
