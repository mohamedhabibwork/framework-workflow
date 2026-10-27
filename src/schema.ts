import type { Actor, JsonObject, JsonValue } from '@framework/core';
import type { InferSelectModel } from 'drizzle-orm';
import { type AnyPgColumn, index, integer, jsonb, pgTable, text, timestamp, uuid } from 'drizzle-orm/pg-core';
import { type ExecutionVia, NODE_KINDS, NODE_STATUSES, RUN_STATUSES } from './types';

/**
 * Framework-owned workflow state. Applications re-export these tables from
 * their schema index so Drizzle Kit generates their migration with the app's
 * other tables (same pattern as `audit_logs`).
 *
 * `workflow_nodes` is the uniform execution tree: one row per pipeline, stage
 * and action of a run; parents reference `workflow_nodes` itself.
 */
export const workflowRuns = pgTable(
  'workflow_runs',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    workflow: text('workflow').notNull(),
    version: integer('version').notNull(),
    input: jsonb('input').$type<JsonObject>().notNull(),
    state: jsonb('state').$type<JsonObject>().notNull().default({}),
    status: text('status', { enum: RUN_STATUSES }).notNull().default('pending'),
    error: text('error'),
    actor: jsonb('actor').$type<Actor>(),
    correlationId: text('correlation_id').notNull(),
    createdAt: timestamp('created_at', { withTimezone: true, mode: 'string' }).notNull().defaultNow(),
    startedAt: timestamp('started_at', { withTimezone: true, mode: 'string' }),
    finishedAt: timestamp('finished_at', { withTimezone: true, mode: 'string' }),
  },
  (table) => [
    index('workflow_runs_workflow_created_idx').on(table.workflow, table.createdAt),
    index('workflow_runs_status_created_idx').on(table.status, table.createdAt),
    index('workflow_runs_correlation_idx').on(table.correlationId),
  ],
);

export type WorkflowRunRow = InferSelectModel<typeof workflowRuns>;

export const workflowNodes = pgTable(
  'workflow_nodes',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    runId: uuid('run_id')
      .notNull()
      .references(() => workflowRuns.id, { onDelete: 'cascade' }),
    parentId: uuid('parent_id').references((): AnyPgColumn => workflowNodes.id, { onDelete: 'cascade' }),
    kind: text('kind', { enum: NODE_KINDS }).notNull(),
    key: text('key').notNull(),
    path: text('path').notNull(),
    nodeIndex: integer('node_index').notNull().default(0),
    groupId: text('group_id'),
    via: text('via', { enum: ['inline', 'queue'] as const })
      .$type<ExecutionVia>()
      .notNull()
      .default('inline'),
    status: text('status', { enum: NODE_STATUSES }).notNull().default('pending'),
    attempts: integer('attempts').notNull().default(0),
    maxAttempts: integer('max_attempts').notNull().default(1),
    output: jsonb('output').$type<JsonValue>(),
    error: text('error'),
    waitEvent: text('wait_event'),
    waitDeadline: timestamp('wait_deadline', { withTimezone: true, mode: 'string' }),
    createdAt: timestamp('created_at', { withTimezone: true, mode: 'string' }).notNull().defaultNow(),
    startedAt: timestamp('started_at', { withTimezone: true, mode: 'string' }),
    finishedAt: timestamp('finished_at', { withTimezone: true, mode: 'string' }),
  },
  (table) => [
    index('workflow_nodes_run_parent_idx').on(table.runId, table.parentId, table.nodeIndex),
    index('workflow_nodes_run_status_idx').on(table.runId, table.status),
    index('workflow_nodes_wait_idx').on(table.waitEvent, table.status),
  ],
);

export type WorkflowNodeRow = InferSelectModel<typeof workflowNodes>;
