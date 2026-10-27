import type { Actor, ExecutionContext, JsonObject, JsonValue, Scope } from '@framework/core';
import type { z } from 'zod/v4';

// -- Execution model ----------------------------------------------------------
//
// Workflow -> Pipeline -> Stage -> Action. Actions are the atomic units; they
// run inline inside the parent's worker or as their own queue job (`via`).
// Stages are the checkpoint boundaries: a stage completes (and its output
// persists) only when every action beneath it reached a terminal state.

/** Where a unit of work executes: inside the parent's worker, or as its own BullMQ job. */
export type ExecutionVia = 'inline' | 'queue';

/** JSON state accumulated across a run; action outputs merge into it. */
export type WorkflowState = JsonObject;

/** Handed to precheck/run/compensate callbacks. Runs inside a `workflow` scope. */
export interface WorkflowContext {
  readonly runId: string;
  readonly nodeId: string;
  readonly attempt: number;
  readonly scope: Scope;
  readonly execution: ExecutionContext;
  readonly signal: AbortSignal;
  readonly state: WorkflowState;
  progress(percent: number): Promise<void>;
}

/**
 * The atomic unit of work. `input` (optional) zod-validates and strips the
 * slice of state the action consumes. `precheck` gates execution: a throw
 * fails the action with `PRECHECK_FAILED` before any side effect. `compensate`
 * runs in reverse completion order when the workflow fails.
 */
export interface ActionDefinition<TInput extends WorkflowState = WorkflowState, TResult extends JsonValue = JsonValue> {
  readonly name: string;
  readonly via?: ExecutionVia;
  /** Total tries (default 3); exponential backoff of `backoffMs` (default 1s). */
  readonly attempts?: number;
  readonly backoffMs?: number;
  readonly timeoutMs?: number;
  /** Gate on state; a false result skips the action without running it. */
  readonly when?: (state: WorkflowState) => boolean;
  readonly precheck?: (state: WorkflowState, context: WorkflowContext) => void | Promise<void>;
  readonly input?: z.ZodType<TInput>;
  run(input: TInput, context: WorkflowContext): Promise<TResult>;
  readonly compensate?: (state: WorkflowState, context: WorkflowContext) => void | Promise<void>;
}

// oxlint-disable-next-line typescript/no-explicit-any -- definitions are heterogeneous; erased at the runner boundary
export type AnyActionDefinition = ActionDefinition<any, any>;

export interface ParallelGroup {
  readonly parallel: true;
  readonly actions: readonly AnyActionDefinition[];
}

/** One entry of `stage.actions`: a single action or a parallel group of them. */
export type StageAction = AnyActionDefinition | ParallelGroup;

/**
 * Checkpoint boundary. Either composed of `actions` (sequential, parallel
 * groups allowed) or a convenience default `run`. `waitFor` parks the whole
 * run (status `waiting`) until a domain event of that type arrives on the
 * event bus; the picked payload becomes the stage output.
 */
export interface StageDefinition {
  readonly name: string;
  readonly via?: ExecutionVia;
  readonly timeoutMs?: number;
  readonly when?: (state: WorkflowState) => boolean;
  readonly precheck?: (state: WorkflowState, context: WorkflowContext) => void | Promise<void>;
  readonly actions?: StageAction | readonly StageAction[];
  readonly run?: (state: WorkflowState, context: WorkflowContext) => JsonValue | Promise<JsonValue>;
  readonly waitFor?: {
    readonly event: string;
    readonly timeoutMs?: number;
    /** Extracts the persisted output from the waking event (default: its payload). */
    readonly pick?: (event: { readonly type: string; readonly payload: JsonObject }) => JsonValue;
  };
}

/** Ordered stages; a pipeline is dispatchable on its own. */
export interface PipelineDefinition {
  readonly name: string;
  readonly via?: ExecutionVia;
  readonly stages: readonly StageDefinition[];
}

/**
 * Versioned, zod-validated entry point. `pipelines` run sequentially; the
 * `stages` shorthand wraps them in one implicit pipeline. On failure the
 * default `compensate` walks completed actions in reverse; `halt` stops
 * without undoing side effects.
 */
export interface WorkflowDefinition<TInput extends JsonObject = JsonObject> {
  readonly name: string;
  readonly version: number;
  readonly input: z.ZodType<TInput>;
  readonly pipelines?: readonly PipelineDefinition[];
  readonly stages?: readonly StageDefinition[];
  readonly onFailure?: 'compensate' | 'halt';
}

// -- Persistence records --------------------------------------------------------

export const RUN_STATUSES = [
  'pending',
  'running',
  'waiting',
  'completed',
  'failed',
  'cancelled',
  'compensating',
  'compensated',
] as const;

export type RunStatus = (typeof RUN_STATUSES)[number];

export const NODE_KINDS = ['pipeline', 'stage', 'action'] as const;

export type NodeKind = (typeof NODE_KINDS)[number];

export const NODE_STATUSES = [
  'pending',
  'running',
  'completed',
  'skipped',
  'failed',
  'waiting',
  'compensated',
] as const;

export type NodeStatus = (typeof NODE_STATUSES)[number];

export interface RunRecord {
  readonly id: string;
  readonly workflow: string;
  readonly version: number;
  readonly input: JsonObject;
  readonly state: WorkflowState;
  readonly status: RunStatus;
  readonly error: string | null;
  readonly actor: Actor | null;
  readonly correlationId: string;
  readonly createdAt: string;
  readonly startedAt: string | null;
  readonly finishedAt: string | null;
}

export interface NodeRecord {
  readonly id: string;
  readonly runId: string;
  readonly parentId: string | null;
  readonly kind: NodeKind;
  readonly key: string;
  /** Human-readable dotted route, e.g. `payments.charge-card`. */
  readonly path: string;
  readonly index: number;
  /** Siblings sharing a groupId run in parallel. */
  readonly groupId: string | null;
  readonly via: ExecutionVia;
  readonly status: NodeStatus;
  readonly attempts: number;
  readonly maxAttempts: number;
  readonly output: JsonValue | null;
  readonly error: string | null;
  readonly waitEvent: string | null;
  readonly waitDeadline: string | null;
  readonly createdAt: string;
  readonly startedAt: string | null;
  readonly finishedAt: string | null;
}

/** Run with its full node tree, as served by `status` and the HTTP API. */
export interface WorkflowRunStatus {
  readonly run: RunRecord;
  readonly nodes: readonly NodeRecord[];
}

const TERMINAL_RUN_STATUSES: readonly RunStatus[] = ['completed', 'failed', 'cancelled', 'compensated'];

export const isTerminalRun = (status: RunStatus): boolean => TERMINAL_RUN_STATUSES.includes(status);
