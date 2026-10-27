import {
  type Application,
  EXECUTION_CONTEXT,
  ExecutionContext,
  FrameworkError,
  type JsonObject,
  type JsonValue,
  runWithExecution,
  type Scope,
  SYSTEM_ACTOR,
} from '@framework/core';
import type { DomainEvent } from '@framework/events';
import type { QueueManager } from '@framework/queue';
import { flattenActions, flattenGroups, type ValidatedWorkflow } from './definition';
import type { WorkflowEvents } from './events';
import { enqueueInternalWorkflowJob, WORKFLOW_ADVANCE_NAME, WORKFLOW_UNIT_NAME } from './internal-jobs';
import type { WorkflowStore } from './store';
import type { ActionDefinition, NodeRecord, RunRecord, StageDefinition, WorkflowState } from './types';
import { isTerminalRun } from './types';

export interface WorkflowRegistryView {
  /** Returns the validated definition when name and version match the run. */
  resolve(name: string, version: number): ValidatedWorkflow | undefined;
}

export interface RunnerOptions {
  /** Default per-action timeout when a definition has none (default 5 minutes). */
  readonly actionTimeoutMs?: number;
  /** Default wait-for-event timeout (default 24 hours). */
  readonly waitTimeoutMs?: number;
  /** Notifies the worker when a stage parks on an event, so the bridge subscribes. */
  readonly onWaitRegistered?: (eventType: string) => void;
}

type ActionOutcome = 'completed' | 'failed' | 'skipped' | 'parked';
type StageOutcome = 'completed' | 'failed' | 'parked';

const DEFAULT_ACTION_TIMEOUT_MS = 5 * 60_000;
const DEFAULT_WAIT_TIMEOUT_MS = 24 * 60 * 60_000;
/** BullMQ attempts of subtree (pipeline/stage) units; leaf actions use their own attempt count. */
const UNIT_ATTEMPTS = 3;
const ADVANCE_ATTEMPTS = 3;

const NODE_TERMINAL: readonly string[] = ['completed', 'skipped', 'failed', 'compensated'];
const NODE_ACTIVE: readonly string[] = ['running', 'waiting'];

const isTerminalNode = (status: string): boolean => NODE_TERMINAL.includes(status);

function normalizeOutput(key: string, result: JsonValue | null | undefined): JsonObject {
  if (result === undefined || result === null) return {};
  if (typeof result === 'object' && !Array.isArray(result)) return result as JsonObject;
  return { [key]: result };
}

function mergeOutputs(nodes: readonly NodeRecord[]): JsonObject {
  const merged: Record<string, JsonValue> = {};
  for (const node of nodes) {
    const output = node.output;
    if (output && typeof output === 'object' && !Array.isArray(output)) Object.assign(merged, output);
  }
  return merged;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Executes workflow runs by walking the persisted node tree. Inline work runs
 * inside the current unit job; `via: 'queue'` units are dispatched as their
 * own `__workflow.unit` jobs and rejoined through `__workflow.advance`
 * evaluations. Every transition is a conditional store update, so
 * re-delivered jobs, crashes and concurrent workers converge on one outcome
 * instead of double-executing.
 */
export class WorkflowRunner {
  constructor(
    private readonly application: Application,
    private readonly store: WorkflowStore,
    private readonly registry: WorkflowRegistryView,
    private readonly queue: QueueManager,
    private readonly events: WorkflowEvents,
    private readonly options: RunnerOptions = {},
  ) {}

  /** Entry point of `__workflow.unit`: executes a node's subtree as far as possible. */
  async executeUnit(runId: string, nodeId: string): Promise<void> {
    const run = await this.store.runById(runId);
    if (!run) throw new FrameworkError('WORKFLOW_RUN_NOT_FOUND', `Workflow run "${runId}" does not exist`);
    if (run.status === 'cancelled' || ['completed', 'failed', 'compensated', 'compensating'].includes(run.status))
      return;

    let node = await this.store.nodeById(nodeId);
    if (!node) throw new FrameworkError('WORKFLOW_NODE_NOT_FOUND', `Workflow node "${nodeId}" does not exist`);
    if (isTerminalNode(node.status) || node.status === 'waiting') return;
    const workflow = this.#requireDefinition(run);

    if (node.status === 'pending') {
      const claimed = await this.store.claimNode(node.id);
      if (!claimed) return; // another worker got there first
      node = claimed;
    } // status 'running' means a re-entrant continuation of this node's unit.

    if (run.status === 'pending') {
      await this.store.transitionRun(run.id, ['pending'], 'running', { started: true });
    }

    const scope = this.application.createScope('workflow');
    const execution = this.#executionFor(run);
    scope.override(EXECUTION_CONTEXT, execution);
    try {
      await runWithExecution(execution, async () => {
        if (node.kind === 'pipeline') await this.#runPipeline(run, node, workflow, scope, execution);
        else if (node.kind === 'stage') await this.#runStage(run, node, workflow, scope, execution);
        else await this.#runQueuedAction(run, node, workflow, scope, execution);
      });
    } finally {
      await scope.dispose();
    }
  }

  /** Entry point of `__workflow.advance`: re-evaluates a node after a child settled. */
  async advance(runId: string, nodeId: string): Promise<void> {
    const run = await this.store.runById(runId);
    if (
      !run ||
      run.status === 'cancelled' ||
      ['completed', 'failed', 'compensated', 'compensating'].includes(run.status)
    )
      return;
    const node = await this.store.nodeById(nodeId);
    if (!node || node.kind === 'action' || isTerminalNode(node.status)) {
      // A stage marked failed by its own unit still owes the run a cascade.
      if (run && node && node.status === 'failed' && !isTerminalRun(run.status)) {
        await this.#failCascade(run, node, node.error ?? `stage "${node.key}" failed`, this.#requireDefinition(run));
      }
      return;
    }
    const workflow = this.#requireDefinition(run);

    const children = await this.store.children(node.id);
    if (children.some((child) => NODE_ACTIVE.includes(child.status))) return; // patience: the last active child re-fires advance

    const failed = children.find((child) => child.status === 'failed');
    if (failed) {
      if (node.status === 'running') await this.store.failNode(node.id, failed.error ?? `child "${failed.key}" failed`);
      await this.#failCascade(run, node, failed.error ?? `child "${failed.key}" failed`, workflow);
      return;
    }

    if (children.some((child) => child.status === 'pending')) {
      // Work remains to be dispatched (e.g. inline members after a queued sibling).
      await this.#enqueueUnit(run, node);
      return;
    }

    const merged = mergeOutputs(children);
    const completed = await this.store.completeNode(node.id, merged);
    if (!completed) return; // someone else completed it
    await this.store.mergeState(runId, merged);
    if (node.kind === 'stage') await this.events.emit(runId, 'stage.completed', { path: node.path });

    const parent = node.parentId ? await this.store.nodeById(node.parentId) : null;
    if (parent && !isTerminalNode(parent.status)) {
      await this.#enqueueAdvance(run, parent.id);
      return;
    }
    if (!parent) await this.#afterRootPipeline(run, node, workflow);
  }

  /** Event-bus bridge: completes parked wait stages whose event just arrived. */
  async onEvent(event: DomainEvent): Promise<void> {
    const waiters = await this.store.dueWaiters(event.type);
    for (const stageNode of waiters) {
      const run = await this.store.runById(stageNode.runId);
      if (run?.status !== 'waiting') continue;
      if (stageNode.waitDeadline && new Date(stageNode.waitDeadline) < new Date()) continue; // the timeout sweep owns expiry
      const workflow = this.#requireDefinition(run);
      const stageDef = await this.#resolveStage(workflow, stageNode);
      const pick = stageDef?.waitFor?.pick;
      const picked = pick
        ? pick({ type: event.type, payload: event.payload as JsonObject })
        : (event.payload as JsonValue);
      const output = normalizeOutput(stageNode.key, picked);
      const completed = await this.store.completeWaiter(stageNode.id, output);
      if (!completed) continue;
      await this.store.transitionRun(run.id, ['waiting'], 'running');
      await this.store.mergeState(run.id, output);
      await this.events.emit(run.id, 'resumed', { stage: stageNode.key, event: event.type });
      await this.events.emit(run.id, 'stage.completed', { path: stageNode.path });
      if (stageNode.parentId) await this.#enqueueAdvance(run, stageNode.parentId);
    }
  }

  /** Fails wait stages past their deadline; called periodically by the worker. */
  async failExpiredWaiters(): Promise<number> {
    const expired = await this.store.expiredWaiters(new Date());
    let failed = 0;
    for (const node of expired) {
      const run = await this.store.runById(node.runId);
      if (run?.status !== 'waiting') continue;
      const workflow = this.#requireDefinition(run);
      const text = `Wait for "${node.waitEvent}" timed out`;
      await this.store.failNode(node.id, text);
      await this.events.emit(run.id, 'stage.failed', { path: node.path, error: text });
      await this.#failCascade(run, node, text, workflow);
      failed += 1;
    }
    return failed;
  }

  // -- Pipeline -----------------------------------------------------------------

  async #runPipeline(
    run: RunRecord,
    pipelineNode: NodeRecord,
    workflow: ValidatedWorkflow,
    scope: Scope,
    execution: ExecutionContext,
  ): Promise<void> {
    const pipelineDef = workflow.pipelines[pipelineNode.index];
    if (!pipelineDef || pipelineDef.name !== pipelineNode.key) {
      await this.#failCascade(run, pipelineNode, `Pipeline drift at "${pipelineNode.path}"`, workflow);
      return;
    }
    for (;;) {
      const children = await this.store.children(pipelineNode.id);
      const failedChild = children.find((child) => child.status === 'failed');
      if (failedChild) {
        await this.#failCascade(run, failedChild, failedChild.error ?? `stage "${failedChild.key}" failed`, workflow);
        return;
      }
      const next = children.find((child) => child.status === 'pending' || NODE_ACTIVE.includes(child.status));
      if (!next) break;
      if (next.status !== 'pending') return; // running/waiting stage: parked until its advance

      if (next.via === 'queue') {
        await this.#enqueueUnit(run, next);
        return;
      }
      const claimed = await this.store.claimNode(next.id);
      if (!claimed) return;
      const outcome = await this.#runStage(run, claimed, workflow, scope, execution);
      if (outcome !== 'completed') return; // 'failed' cascades via advance; 'parked' waits for advance
    }

    const children = await this.store.children(pipelineNode.id);
    const merged = mergeOutputs(children);
    const completed = await this.store.completeNode(pipelineNode.id, merged);
    if (!completed) return;
    await this.store.mergeState(run.id, merged);
    await this.#afterRootPipeline(run, pipelineNode, workflow);
  }

  /** Sequential root pipelines: start the next one, or finish the run. */
  async #afterRootPipeline(run: RunRecord, pipelineNode: NodeRecord, workflow: ValidatedWorkflow): Promise<void> {
    const nextIndex = pipelineNode.index + 1;
    if (nextIndex < workflow.pipelines.length) {
      const next = await this.store.rootNodeByIndex(run.id, nextIndex);
      if (next && !isTerminalNode(next.status)) {
        await this.#enqueueUnit(run, next);
        return;
      }
    }
    const finished = await this.store.transitionRun(run.id, ['running', 'pending'], 'completed', { finished: true });
    if (finished) await this.events.emit(run.id, 'completed', { workflow: run.workflow });
  }

  // -- Stage ----------------------------------------------------------------------

  async #runStage(
    run: RunRecord,
    stageNode: NodeRecord,
    workflow: ValidatedWorkflow,
    scope: Scope,
    execution: ExecutionContext,
  ): Promise<StageOutcome> {
    const stageDef = await this.#resolveStage(workflow, stageNode);
    if (!stageDef) {
      const text = `Stage drift at "${stageNode.path}"`;
      await this.store.failNode(stageNode.id, text);
      await this.#failCascade(run, stageNode, text, workflow);
      return 'failed';
    }

    // A wait stage parks the whole run until its event arrives (or times out).
    if (stageDef.waitFor) {
      const timeoutMs = stageDef.waitFor.timeoutMs ?? this.options.waitTimeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS;
      const parked = await this.store.parkNode(stageNode.id, stageDef.waitFor.event, new Date(Date.now() + timeoutMs));
      if (parked) {
        await this.store.transitionRun(run.id, ['running', 'pending'], 'waiting');
        await this.events.emit(run.id, 'waiting', { stage: stageNode.key, event: stageDef.waitFor.event });
        this.options.onWaitRegistered?.(stageDef.waitFor.event);
      }
      return 'parked';
    }

    const state = await this.#stateOf(run);
    const context = this.#context(run, stageNode, stageNode.attempts, scope, execution, state);

    if (stageDef.when && !stageDef.when(state)) {
      await this.store.skipNode(stageNode.id);
      return 'completed';
    }
    if (stageDef.precheck) {
      try {
        await stageDef.precheck(state, context);
      } catch (error) {
        const text = `PRECHECK_FAILED: ${messageOf(error)}`;
        await this.store.failNode(stageNode.id, text);
        await this.events.emit(run.id, 'stage.failed', { path: stageNode.path, error: text });
        await this.#failCascade(run, stageNode, text, workflow);
        return 'failed';
      }
    }

    const actions = flattenActions(stageDef);
    const groups = flattenGroups(stageDef);
    const children = await this.store.children(stageNode.id);

    for (let i = 0; i < actions.length; i += 1) {
      const actionNode = children[i];
      const actionDef = actions[i];
      if (!actionNode || !actionDef || actionNode.key !== actionDef.name) {
        const text = `Action drift at stage "${stageNode.path}"`;
        await this.store.failNode(stageNode.id, text);
        await this.#failCascade(run, stageNode, text, workflow);
        return 'failed';
      }
      if (isTerminalNode(actionNode.status)) continue;
      if (NODE_ACTIVE.includes(actionNode.status)) return 'parked';

      let groupEnd = i;
      if (groups[i]) while (groupEnd + 1 < groups.length && groups[groupEnd + 1] === groups[i]) groupEnd += 1;
      const groupNodes = children.slice(i, groupEnd + 1);
      const groupDefs = actions.slice(i, groupEnd + 1);

      const pendingQueued = groupNodes.filter((node) => node.status === 'pending' && node.via === 'queue');
      if (pendingQueued.length > 0) {
        for (const node of pendingQueued) await this.#enqueueUnit(run, node);
        return 'parked'; // group resumes through advance once the queued members settle
      }
      if (NODE_ACTIVE.some((status) => groupNodes.some((node) => node.status === status))) return 'parked';

      const pendingInline = groupNodes
        .map((node, offset) => ({ node, def: groupDefs[offset] as ActionDefinition }))
        .filter((pair) => pair.node.status === 'pending');
      if (pendingInline.length > 0) {
        const outcomes = await Promise.all(
          pendingInline.map((pair) => this.#runInlineAction(run, pair.node, pair.def, scope, execution)),
        );
        if (outcomes.includes('failed')) {
          const text = `action "${pendingInline[0]?.node.key ?? 'unknown'}" failed`;
          await this.store.failNode(stageNode.id, text);
          await this.#failCascade(run, stageNode, text, workflow);
          return 'failed';
        }
        if (outcomes.includes('parked')) return 'parked';
      }
      i = groupEnd;
    }

    const finalChildren = await this.store.children(stageNode.id);
    if (finalChildren.some((child) => NODE_ACTIVE.includes(child.status) || child.status === 'pending'))
      return 'parked';
    const merged = mergeOutputs(finalChildren);
    const completed = await this.store.completeNode(stageNode.id, merged);
    if (!completed) return 'completed';
    await this.store.mergeState(run.id, merged);
    await this.events.emit(run.id, 'stage.completed', { path: stageNode.path });
    // Whether this stage ran inline (parent pipeline still looping in this unit)
    // or as its own continuation unit, the pipeline learns the news via advance.
    // Its conditional transitions make a duplicate continuation harmless.
    await this.#enqueueAdvance(run, stageNode.parentId);
    return 'completed';
  }

  // -- Actions ---------------------------------------------------------------------

  /** Runs one action inline (with its own retry loop); claim loss defers to the other worker. */
  async #runInlineAction(
    run: RunRecord,
    actionNode: NodeRecord,
    actionDef: ActionDefinition,
    scope: Scope,
    execution: ExecutionContext,
  ): Promise<ActionOutcome> {
    const claimed = await this.store.claimNode(actionNode.id);
    if (!claimed) return 'parked'; // another worker owns this action

    const state = await this.#stateOf(run);
    if (actionDef.when && !actionDef.when(state)) {
      await this.store.skipNode(claimed.id);
      await this.events.emit(run.id, 'action.skipped', { action: claimed.path });
      return 'skipped';
    }
    if (actionDef.precheck) {
      try {
        await actionDef.precheck(state, this.#context(run, claimed, 0, scope, execution, state));
      } catch (error) {
        const text = `PRECHECK_FAILED: ${messageOf(error)}`;
        await this.store.failNode(claimed.id, text);
        await this.events.emit(run.id, 'action.failed', { action: claimed.path, error: text });
        return 'failed';
      }
    }

    const attempts = actionDef.attempts ?? 3;
    const backoffMs = actionDef.backoffMs ?? 1_000;
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      const outcome = await this.#attempt(run, claimed, actionDef, attempt, attempts, scope, execution);
      if (outcome === 'retry') {
        if (attempt < attempts) await Bun.sleep(backoffMs * attempt);
        continue;
      }
      return outcome;
    }
    return 'failed';
  }

  /** One attempt of an inline action: run, checkpoint, record. */
  async #attempt(
    run: RunRecord,
    node: NodeRecord,
    actionDef: ActionDefinition,
    attempt: number,
    attempts: number,
    scope: Scope,
    execution: ExecutionContext,
  ): Promise<ActionOutcome | 'retry'> {
    const timeoutMs = actionDef.timeoutMs ?? this.options.actionTimeoutMs ?? DEFAULT_ACTION_TIMEOUT_MS;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const state = await this.#stateOf(run);
      const context = this.#context(run, node, attempt, scope, execution, state, controller.signal);
      const input = (actionDef.input ? actionDef.input.parse(state) : state) as never;
      const result = await actionDef.run(input, context);
      const output = normalizeOutput(node.key, result as JsonValue);
      const completed = await this.store.completeNode(node.id, output);
      if (!completed) return 'skipped'; // another worker finished it
      await this.store.mergeState(run.id, output);
      await this.events.emit(run.id, 'action.completed', { action: node.path, attempt });
      return 'completed';
    } catch (error) {
      const text = controller.signal.aborted ? `Action timed out after ${timeoutMs}ms` : messageOf(error);
      if (attempt >= attempts) {
        await this.store.failNode(node.id, text);
        await this.events.emit(run.id, 'action.failed', { action: node.path, attempt, error: text });
        return 'failed';
      }
      await this.store.requeueNode(node.id, text);
      return 'retry';
    } finally {
      clearTimeout(timer);
    }
  }

  /** Queued actions run as their own `__workflow.unit` job (one BullMQ attempt per try). */
  async #runQueuedAction(
    run: RunRecord,
    node: NodeRecord,
    workflow: ValidatedWorkflow,
    scope: Scope,
    execution: ExecutionContext,
  ): Promise<void> {
    const actionDef = await this.#resolveAction(workflow, node);
    const failAndAdvance = async (text: string): Promise<void> => {
      await this.store.failNode(node.id, text);
      await this.events.emit(run.id, 'action.failed', { action: node.path, error: text });
      await this.#enqueueAdvance(run, node.parentId);
    };
    if (!actionDef) {
      await failAndAdvance(`Action drift at "${node.path}"`);
      return;
    }

    const state = await this.#stateOf(run);
    if (actionDef.when && !actionDef.when(state)) {
      await this.store.skipNode(node.id);
      await this.events.emit(run.id, 'action.skipped', { action: node.path });
      return;
    }
    if (actionDef.precheck) {
      try {
        await actionDef.precheck(state, this.#context(run, node, node.attempts, scope, execution, state));
      } catch (error) {
        await failAndAdvance(`PRECHECK_FAILED: ${messageOf(error)}`);
        return;
      }
    }

    const timeoutMs = actionDef.timeoutMs ?? this.options.actionTimeoutMs ?? DEFAULT_ACTION_TIMEOUT_MS;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const context = this.#context(run, node, node.attempts, scope, execution, state, controller.signal);
      const input = (actionDef.input ? actionDef.input.parse(state) : state) as never;
      const result = await actionDef.run(input, context);
      const output = normalizeOutput(node.key, result as JsonValue);
      const completed = await this.store.completeNode(node.id, output);
      if (completed) {
        await this.store.mergeState(run.id, output);
        await this.events.emit(run.id, 'action.completed', { action: node.path, attempt: node.attempts });
      }
    } catch (error) {
      const text = controller.signal.aborted ? `Action timed out after ${timeoutMs}ms` : messageOf(error);
      if (node.attempts < node.maxAttempts) {
        await this.store.requeueNode(node.id, text); // BullMQ retries this unit job
        throw error;
      }
      await failAndAdvance(text);
      return;
    } finally {
      clearTimeout(timer);
    }
    await this.#enqueueAdvance(run, node.parentId);
  }

  // -- Failure and compensation ------------------------------------------------------

  /**
   * Ends the run after a failure. With `onFailure: 'halt'` the run simply
   * fails; otherwise it walks completed actions newest-first running their
   * `compensate` handlers. Idempotent: only the caller that wins the
   * `compensating` transition performs the walk.
   */
  async #failCascade(
    run: RunRecord,
    failingNode: NodeRecord,
    text: string,
    workflow: ValidatedWorkflow,
  ): Promise<void> {
    if (workflow.workflow.onFailure === 'halt') {
      const failed = await this.store.transitionRun(run.id, ['running', 'waiting', 'pending'], 'failed', {
        error: text,
        finished: true,
      });
      if (failed) {
        await this.events.emit(run.id, 'failed', { workflow: run.workflow, error: text, path: failingNode.path });
      }
      return;
    }
    const started = await this.store.transitionRun(run.id, ['running', 'waiting', 'pending'], 'compensating', {
      error: text,
    });
    if (!started) return; // another worker is compensating (or already done)
    await this.events.emit(run.id, 'compensating', { path: failingNode.path, error: text });

    const scope = this.application.createScope('workflow');
    scope.override(EXECUTION_CONTEXT, this.#executionFor(run));
    try {
      const actions = await this.store.completedActionNodes(run.id);
      for (const actionNode of actions) {
        const actionDef = await this.#resolveAction(workflow, actionNode);
        if (!actionDef?.compensate) continue;
        const execution = this.#executionFor(run);
        try {
          await runWithExecution(execution, async () => {
            const state = await this.#stateOf(run);
            await actionDef.compensate?.(state, this.#context(run, actionNode, 0, scope, execution, state));
          });
          await this.store.compensateNode(actionNode.id);
          await this.events.emit(run.id, 'action.compensated', { action: actionNode.path });
        } catch (error) {
          // A failing compensation never halts the walk; the run still ends compensated.
          console.error(`[workflow] compensation failed for "${actionNode.path}"`, error);
        }
      }
    } finally {
      await scope.dispose();
    }
    const done = await this.store.transitionRun(run.id, ['compensating'], 'compensated', { finished: true });
    if (done) await this.events.emit(run.id, 'compensated', { workflow: run.workflow });
  }

  // -- Internal job dispatch ------------------------------------------------------

  async #enqueueUnit(run: RunRecord, node: NodeRecord): Promise<void> {
    // Leaf actions carry their own attempt count; subtree units retry a bounded few times.
    const attempts = node.kind === 'action' ? Math.max(1, node.maxAttempts) : UNIT_ATTEMPTS;
    await enqueueInternalWorkflowJob(
      this.queue.queue,
      this.queue.options,
      WORKFLOW_UNIT_NAME,
      { runId: run.id, nodeId: node.id },
      { correlationId: run.correlationId, actor: run.actor ?? SYSTEM_ACTOR },
      attempts,
    );
  }

  async #enqueueAdvance(run: RunRecord, nodeId: string | null): Promise<void> {
    if (!nodeId) return;
    await enqueueInternalWorkflowJob(
      this.queue.queue,
      this.queue.options,
      WORKFLOW_ADVANCE_NAME,
      { runId: run.id, nodeId },
      { correlationId: run.correlationId, actor: run.actor ?? SYSTEM_ACTOR },
      ADVANCE_ATTEMPTS,
    );
  }

  // -- Resolution helpers ---------------------------------------------------------

  /** Walks parent links once and resolves the definitions a node refers to. */
  async #resolveChain(
    workflow: ValidatedWorkflow,
    node: NodeRecord,
  ): Promise<{ stageDef: StageDefinition | null; actionDef: ActionDefinition | null }> {
    const chain: NodeRecord[] = [node];
    let current = node;
    while (current.parentId) {
      const parent = await this.store.nodeById(current.parentId);
      if (!parent) break;
      chain.push(parent);
      current = parent;
    }
    const root = chain[chain.length - 1];
    if (root?.kind !== 'pipeline') return { stageDef: null, actionDef: null };
    const pipelineDef = workflow.pipelines[root.index];
    if (!pipelineDef || pipelineDef.name !== root.key) return { stageDef: null, actionDef: null };

    const stageNode = chain.find((n) => n.kind === 'stage');
    if (!stageNode) return { stageDef: null, actionDef: null };
    const stageCandidate = pipelineDef.stages[stageNode.index];
    const stageDef = stageCandidate && stageCandidate.name === stageNode.key ? stageCandidate : null;
    if (!stageDef) return { stageDef: null, actionDef: null };

    const actionNode = chain.find((n) => n.kind === 'action');
    if (!actionNode) return { stageDef, actionDef: null };
    const actionCandidate = flattenActions(stageDef)[actionNode.index];
    const actionDef = actionCandidate && actionCandidate.name === actionNode.key ? actionCandidate : null;
    return { stageDef, actionDef };
  }

  #resolveStage(workflow: ValidatedWorkflow, stageNode: NodeRecord): Promise<StageDefinition | null> {
    return this.#resolveChain(workflow, stageNode).then((r) => r.stageDef);
  }

  #resolveAction(workflow: ValidatedWorkflow, actionNode: NodeRecord): Promise<ActionDefinition | null> {
    return this.#resolveChain(workflow, actionNode).then((r) => r.actionDef);
  }

  #executionFor(run: RunRecord): ExecutionContext {
    return new ExecutionContext('workflow', run.correlationId, run.actor ?? SYSTEM_ACTOR);
  }

  #context(
    run: RunRecord,
    node: NodeRecord,
    attempt: number,
    scope: Scope,
    execution: ExecutionContext,
    state: WorkflowState,
    signal: AbortSignal = new AbortController().signal,
  ) {
    return {
      runId: run.id,
      nodeId: node.id,
      attempt,
      scope,
      execution,
      signal,
      state,
      progress: async (percent: number) => {
        await this.events.emit(run.id, 'action.completed', { action: node.path, progress: percent });
      },
    };
  }

  async #stateOf(run: RunRecord): Promise<WorkflowState> {
    const fresh = await this.store.runById(run.id);
    return fresh?.state ?? run.state;
  }

  #requireDefinition(run: RunRecord): ValidatedWorkflow {
    const workflow = this.registry.resolve(run.workflow, run.version);
    if (!workflow) {
      throw new FrameworkError(
        'WORKFLOW_DEFINITION_UNKNOWN',
        `Workflow "${run.workflow}" v${run.version} is not registered on this worker`,
      );
    }
    return workflow;
  }
}
