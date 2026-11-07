import type { Actor, JsonObject, JsonValue } from '@framework/core';
import { type DbExecutor, type Page, paginationQuerySchema, toPage } from '@framework/database';
import { and, asc, count, desc, eq, inArray, isNotNull, isNull, lt, lte, or, sql } from 'drizzle-orm';
import { z } from 'zod/v4';
import { type WorkflowNodeRow, type WorkflowRunRow, workflowNodes, workflowRuns } from './schema';
import type { ExecutionVia, NodeRecord, RunRecord, RunStatus, WorkflowState } from './types';

export const workflowQuerySchema = paginationQuerySchema().extend({
  workflow: z.string().min(1).max(120).optional(),
  status: z
    .enum(['pending', 'running', 'waiting', 'completed', 'failed', 'cancelled', 'compensating', 'compensated'])
    .optional(),
  correlationId: z.string().min(1).max(200).optional(),
});

export type WorkflowQuery = z.output<typeof workflowQuerySchema>;

export interface NewActionNode {
  readonly key: string;
  readonly path: string;
  readonly via: ExecutionVia;
  readonly groupId: string | null;
  readonly nodeIndex: number;
  readonly maxAttempts: number;
}

export interface NewStageNode {
  readonly key: string;
  readonly path: string;
  readonly via: ExecutionVia;
  readonly nodeIndex: number;
  readonly maxAttempts: number;
  readonly waitEvent?: string | null;
  readonly actions: readonly NewActionNode[];
}

export interface NewPipelineNode {
  readonly key: string;
  readonly path: string;
  readonly via: ExecutionVia;
  readonly nodeIndex: number;
  readonly stages: readonly NewStageNode[];
}

const TERMINAL_RUN: readonly RunStatus[] = ['completed', 'failed', 'cancelled', 'compensated'];

const toRecord = (row: WorkflowRunRow): RunRecord => ({
  id: row.id,
  workflow: row.workflow,
  version: row.version,
  input: row.input,
  state: row.state as WorkflowState,
  status: row.status,
  error: row.error,
  actor: (row.actor as Actor | null) ?? null,
  correlationId: row.correlationId,
  createdAt: row.createdAt,
  startedAt: row.startedAt,
  finishedAt: row.finishedAt,
});

const toNode = (row: WorkflowNodeRow): NodeRecord => ({
  id: row.id,
  runId: row.runId,
  parentId: row.parentId,
  kind: row.kind,
  key: row.key,
  path: row.path,
  index: row.nodeIndex,
  groupId: row.groupId,
  via: row.via,
  status: row.status,
  attempts: row.attempts,
  maxAttempts: row.maxAttempts,
  output: row.output ?? null,
  error: row.error,
  waitEvent: row.waitEvent,
  waitDeadline: row.waitDeadline,
  createdAt: row.createdAt,
  startedAt: row.startedAt,
  finishedAt: row.finishedAt,
});

/**
 * Postgres persistence for workflow runs. Every transition is a conditional
 * UPDATE (... WHERE status IN (...)), so claims, completions and failure
 * cascades are idempotent: concurrent workers can never double-run a node.
 */
export class WorkflowStore {
  constructor(private readonly db: DbExecutor) {}

  async transaction<T>(work: (tx: DbExecutor) => Promise<T>): Promise<T> {
    return this.db.transaction(work);
  }

  /**
   * Inserts the run and its full node tree atomically, level by level so
   * parents exist before children reference them.
   */
  async createRun(
    data: {
      workflow: string;
      version: number;
      input: JsonObject;
      state: WorkflowState;
      actor: Actor | null;
      correlationId: string;
    },
    tree: readonly NewPipelineNode[],
  ): Promise<{ run: RunRecord; nodes: NodeRecord[] }> {
    return this.transaction(async (tx) => {
      const [runRow] = await tx.insert(workflowRuns).values(data).returning();
      const runId = (runRow as WorkflowRunRow).id;
      const inserted: NodeRecord[] = [];

      const pipelineRows = await tx
        .insert(workflowNodes)
        .values(
          tree.map((pipeline) => ({
            runId,
            parentId: null,
            kind: 'pipeline' as const,
            key: pipeline.key,
            path: pipeline.path,
            nodeIndex: pipeline.nodeIndex,
            groupId: null,
            via: pipeline.via,
            maxAttempts: 1,
          })),
        )
        .returning();
      inserted.push(...pipelineRows.map(toNode));

      const stageValues = tree.flatMap((pipeline, pipelineIndex) =>
        pipeline.stages.map((stage) => ({
          runId,
          parentId: pipelineRows[pipelineIndex]?.id ?? null,
          kind: 'stage' as const,
          key: stage.key,
          path: stage.path,
          nodeIndex: stage.nodeIndex,
          groupId: null,
          via: stage.via,
          maxAttempts: stage.maxAttempts,
          waitEvent: stage.waitEvent ?? null,
        })),
      );
      const stageRows = stageValues.length > 0 ? await tx.insert(workflowNodes).values(stageValues).returning() : [];
      inserted.push(...stageRows.map(toNode));

      const actionValues = tree.flatMap((pipeline, pipelineIndex) =>
        pipeline.stages.flatMap((stage) => {
          const stageRow = stageRows.find(
            (row) => row.parentId === pipelineRows[pipelineIndex]?.id && row.nodeIndex === stage.nodeIndex,
          );
          if (!stageRow) return [];
          return stage.actions.map((action) => ({
            runId,
            parentId: stageRow.id,
            kind: 'action' as const,
            key: action.key,
            path: action.path,
            nodeIndex: action.nodeIndex,
            groupId: action.groupId,
            via: action.via,
            maxAttempts: action.maxAttempts,
          }));
        }),
      );
      const actionRows = actionValues.length > 0 ? await tx.insert(workflowNodes).values(actionValues).returning() : [];
      inserted.push(...actionRows.map(toNode));

      return { run: toRecord(runRow as WorkflowRunRow), nodes: inserted };
    });
  }

  async runById(id: string): Promise<RunRecord | null> {
    const rows = await this.db.select().from(workflowRuns).where(eq(workflowRuns.id, id)).limit(1);
    return rows[0] ? toRecord(rows[0]) : null;
  }

  /** Full uuid or unique id prefix; throws `WORKFLOW_RUN_AMBIGUOUS` on several matches. */
  async runByIdOrPrefix(idOrPrefix: string): Promise<RunRecord | null> {
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(idOrPrefix)) {
      return this.runById(idOrPrefix);
    }
    const rows = await this.db
      .select()
      .from(workflowRuns)
      .where(sql`${workflowRuns.id}::text like ${`${idOrPrefix}%`}`)
      .orderBy(desc(workflowRuns.createdAt))
      .limit(2);
    if (rows.length > 1) {
      throw new Error(`Run prefix "${idOrPrefix}" matches multiple runs`);
    }
    return rows[0] ? toRecord(rows[0]) : null;
  }

  async nodeById(id: string): Promise<NodeRecord | null> {
    const rows = await this.db.select().from(workflowNodes).where(eq(workflowNodes.id, id)).limit(1);
    return rows[0] ? toNode(rows[0]) : null;
  }

  async nodesByRun(runId: string): Promise<NodeRecord[]> {
    const rows = await this.db
      .select()
      .from(workflowNodes)
      .where(eq(workflowNodes.runId, runId))
      .orderBy(asc(workflowNodes.createdAt));
    return rows.map(toNode);
  }

  async children(parentId: string): Promise<NodeRecord[]> {
    const rows = await this.db
      .select()
      .from(workflowNodes)
      .where(eq(workflowNodes.parentId, parentId))
      .orderBy(asc(workflowNodes.nodeIndex));
    return rows.map(toNode);
  }

  // -- Node transitions ---------------------------------------------------------

  /** `pending -> running`; returns null when another worker got there first. */
  async claimNode(id: string): Promise<NodeRecord | null> {
    const rows = await this.db
      .update(workflowNodes)
      .set({ status: 'running', startedAt: sql`now()`, attempts: sql`${workflowNodes.attempts} + 1` })
      .where(and(eq(workflowNodes.id, id), eq(workflowNodes.status, 'pending')))
      .returning();
    return rows[0] ? toNode(rows[0]) : null;
  }

  async completeNode(id: string, output: JsonValue): Promise<NodeRecord | null> {
    const rows = await this.db
      .update(workflowNodes)
      .set({ status: 'completed', output, finishedAt: sql`now()` })
      .where(and(eq(workflowNodes.id, id), eq(workflowNodes.status, 'running')))
      .returning();
    return rows[0] ? toNode(rows[0]) : null;
  }

  async skipNode(id: string): Promise<NodeRecord | null> {
    const rows = await this.db
      .update(workflowNodes)
      .set({ status: 'skipped', finishedAt: sql`now()` })
      .where(and(eq(workflowNodes.id, id), inArray(workflowNodes.status, ['pending', 'running'])))
      .returning();
    return rows[0] ? toNode(rows[0]) : null;
  }

  /** Returns the node to a pending state for its next attempt. */
  async requeueNode(id: string, error: string): Promise<NodeRecord | null> {
    const rows = await this.db
      .update(workflowNodes)
      .set({ status: 'pending', error })
      .where(and(eq(workflowNodes.id, id), eq(workflowNodes.status, 'running')))
      .returning();
    return rows[0] ? toNode(rows[0]) : null;
  }

  async failNode(id: string, error: string): Promise<NodeRecord | null> {
    const rows = await this.db
      .update(workflowNodes)
      .set({ status: 'failed', error, finishedAt: sql`now()` })
      .where(and(eq(workflowNodes.id, id), inArray(workflowNodes.status, ['pending', 'running', 'waiting'])))
      .returning();
    return rows[0] ? toNode(rows[0]) : null;
  }

  async parkNode(id: string, waitEvent: string, waitDeadline: Date): Promise<NodeRecord | null> {
    const rows = await this.db
      .update(workflowNodes)
      .set({ status: 'waiting', waitEvent, waitDeadline: waitDeadline.toISOString() })
      .where(and(eq(workflowNodes.id, id), inArray(workflowNodes.status, ['pending', 'running'])))
      .returning();
    return rows[0] ? toNode(rows[0]) : null;
  }

  async compensateNode(id: string): Promise<NodeRecord | null> {
    const rows = await this.db
      .update(workflowNodes)
      .set({ status: 'compensated', finishedAt: sql`now()` })
      .where(and(eq(workflowNodes.id, id), eq(workflowNodes.status, 'completed')))
      .returning();
    return rows[0] ? toNode(rows[0]) : null;
  }

  /** A satisfied wait completes the parked stage directly with the picked payload. */
  async completeWaiter(id: string, output: JsonValue): Promise<NodeRecord | null> {
    const rows = await this.db
      .update(workflowNodes)
      .set({ status: 'completed', output, waitEvent: null, waitDeadline: null, finishedAt: sql`now()` })
      .where(and(eq(workflowNodes.id, id), eq(workflowNodes.status, 'waiting')))
      .returning();
    return rows[0] ? toNode(rows[0]) : null;
  }

  /** Root node (parentId is null) of the given kind at a tree position. */
  async rootNodeByIndex(runId: string, index: number): Promise<NodeRecord | null> {
    const rows = await this.db
      .select()
      .from(workflowNodes)
      .where(and(eq(workflowNodes.runId, runId), isNull(workflowNodes.parentId), eq(workflowNodes.nodeIndex, index)))
      .limit(1);
    return rows[0] ? toNode(rows[0]) : null;
  }

  /** All currently parked stages (used to (re)arm event subscriptions). */
  async waitingStages(): Promise<NodeRecord[]> {
    const rows = await this.db.select().from(workflowNodes).where(eq(workflowNodes.status, 'waiting'));
    return rows.map(toNode);
  }

  /** Stages completed before the failure, newest first — the compensation order. */
  async completedActionNodes(runId: string): Promise<NodeRecord[]> {
    const rows = await this.db
      .select()
      .from(workflowNodes)
      .where(
        and(eq(workflowNodes.runId, runId), eq(workflowNodes.kind, 'action'), eq(workflowNodes.status, 'completed')),
      )
      .orderBy(desc(workflowNodes.finishedAt), asc(workflowNodes.nodeIndex));
    return rows.map(toNode);
  }

  // -- Run transitions and state ------------------------------------------------

  async transitionRun(
    id: string,
    from: readonly RunStatus[],
    to: RunStatus,
    extra: { error?: string; started?: boolean; finished?: boolean } = {},
  ): Promise<RunRecord | null> {
    const rows = await this.db
      .update(workflowRuns)
      .set({
        status: to,
        ...(extra.error !== undefined ? { error: extra.error } : {}),
        ...(extra.started ? { startedAt: sql`now()` } : {}),
        ...(extra.finished ? { finishedAt: sql`now()` } : {}),
      })
      .where(and(eq(workflowRuns.id, id), inArray(workflowRuns.status, [...from])))
      .returning();
    return rows[0] ? toRecord(rows[0]) : null;
  }

  /** Merges a partial output into the run state (jsonb concat). */
  async mergeState(runId: string, patch: JsonObject): Promise<void> {
    await this.db
      .update(workflowRuns)
      .set({ state: sql`${workflowRuns.state} || ${JSON.stringify(patch)}::jsonb` })
      .where(eq(workflowRuns.id, runId));
  }

  // -- Queries for the runner -----------------------------------------------------

  async listRuns(query: WorkflowQuery): Promise<Page<RunRecord>> {
    const { page, limit, workflow, status, correlationId } = query;
    const conditions = [
      ...(workflow ? [eq(workflowRuns.workflow, workflow)] : []),
      ...(status ? [eq(workflowRuns.status, status)] : []),
      ...(correlationId ? [eq(workflowRuns.correlationId, correlationId)] : []),
    ];
    const where = conditions.length > 0 ? and(...conditions) : undefined;
    const [rows, totals] = await Promise.all([
      this.db
        .select()
        .from(workflowRuns)
        .where(where)
        .orderBy(desc(workflowRuns.createdAt))
        .limit(limit)
        .offset((page - 1) * limit),
      this.db.select({ total: count() }).from(workflowRuns).where(where),
    ]);
    return toPage(rows.map(toRecord), totals[0]?.total ?? 0, { page, limit });
  }

  /** Waiting stages listening for the given event type (their run is `waiting`). */
  async dueWaiters(event: string): Promise<NodeRecord[]> {
    const rows = await this.db
      .select({ node: workflowNodes })
      .from(workflowNodes)
      .innerJoin(workflowRuns, eq(workflowNodes.runId, workflowRuns.id))
      .where(
        and(
          eq(workflowNodes.status, 'waiting'),
          eq(workflowNodes.waitEvent, event),
          eq(workflowRuns.status, 'waiting'),
        ),
      );
    return rows.map((r) => toNode(r.node));
  }

  /** Waiting stages whose deadline has passed (their run is `waiting`). */
  async expiredWaiters(now: Date): Promise<NodeRecord[]> {
    const rows = await this.db
      .select({ node: workflowNodes })
      .from(workflowNodes)
      .innerJoin(workflowRuns, eq(workflowNodes.runId, workflowRuns.id))
      .where(
        and(
          eq(workflowNodes.status, 'waiting'),
          eq(workflowRuns.status, 'waiting'),
          isNotNull(workflowNodes.waitDeadline),
          lte(workflowNodes.waitDeadline, now.toISOString()),
        ),
      );
    return rows.map((r) => toNode(r.node));
  }

  /** First failed node in tree order — the default retry entry point. */
  async firstFailedNode(runId: string): Promise<NodeRecord | null> {
    const rows = await this.db
      .select()
      .from(workflowNodes)
      .where(and(eq(workflowNodes.runId, runId), eq(workflowNodes.status, 'failed')))
      .orderBy(asc(workflowNodes.path))
      .limit(1);
    return rows[0] ? toNode(rows[0]) : null;
  }

  /** Marks a failed (or terminal-cascade) subtree runnable again. */
  async resetSubtree(runId: string, nodeId: string): Promise<void> {
    await this.transaction(async (tx) => {
      const [node] = await tx.select().from(workflowNodes).where(eq(workflowNodes.id, nodeId)).limit(1);
      if (!node) return;
      // Descendants: everything whose path extends the node's path.
      await tx
        .update(workflowNodes)
        .set({ status: 'pending', error: null, output: null, startedAt: null, finishedAt: null })
        .where(
          and(
            eq(workflowNodes.runId, runId),
            or(eq(workflowNodes.id, nodeId), sql`${workflowNodes.path} like ${`${node.path}.%`}`),
          ),
        );
    });
  }

  /** Deletes terminal runs finished before `finishedBefore`; returns removed ids. */
  async prune(finishedBefore: Date): Promise<string[]> {
    const rows = await this.db
      .delete(workflowRuns)
      .where(
        and(
          inArray(workflowRuns.status, [...TERMINAL_RUN]),
          isNotNull(workflowRuns.finishedAt),
          lt(workflowRuns.finishedAt, finishedBefore.toISOString()),
        ),
      )
      .returning({ id: workflowRuns.id });
    return rows.map((r) => r.id);
  }

  /** Finds a node of a run by its dotted path or id (retry targeting). */
  async findNode(runId: string, idOrPath: string): Promise<NodeRecord | null> {
    const rows = await this.db
      .select()
      .from(workflowNodes)
      .where(and(eq(workflowNodes.runId, runId), or(eq(workflowNodes.id, idOrPath), eq(workflowNodes.path, idOrPath))))
      .limit(1);
    return rows[0] ? toNode(rows[0]) : null;
  }

  /** Revives failed ancestors of a node so the tree can continue above it. */
  async reviveAncestors(nodeId: string): Promise<void> {
    let current = await this.nodeById(nodeId);
    while (current?.parentId) {
      const parent = await this.nodeById(current.parentId);
      if (!parent) break;
      if (parent.status === 'failed') {
        await this.db
          .update(workflowNodes)
          .set({ status: 'running', error: null, finishedAt: null })
          .where(and(eq(workflowNodes.id, parent.id), eq(workflowNodes.status, 'failed')));
      }
      current = parent;
    }
  }
}
