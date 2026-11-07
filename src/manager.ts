import { type ExecutionContext, FrameworkError, type JsonObject, SYSTEM_ACTOR, token } from '@framework/core';
import type { Page } from '@framework/database';
import type { EventBus } from '@framework/events';
import type { QueueManager } from '@framework/queue';
import { z } from 'zod/v4';
import { stageEntries, type ValidatedWorkflow, validateWorkflow } from './definition';
import { WorkflowEvents } from './events';
import { enqueueInternalWorkflowJob, WORKFLOW_ADVANCE_NAME, WORKFLOW_UNIT_NAME } from './internal-jobs';
import type { WorkflowQuery, WorkflowStore } from './store';
import type {
  NodeRecord,
  PipelineDefinition,
  RunRecord,
  StageDefinition,
  WorkflowDefinition,
  WorkflowRunStatus,
} from './types';

export const WORKFLOW_MANAGER = token<WorkflowManager>('workflow.manager');

export interface WorkflowDispatchOptions {
  readonly correlationId?: string;
}

/**
 * The registry and control plane of the workflow engine: registers
 * definitions (fail-fast, like the job and middleware registries), dispatches
 * runs, and answers status/cancel/retry/resume/prune. The queue is optional
 * so inspection-only consumers (CLI, HTTP) can work without Redis.
 */
export class WorkflowManager {
  readonly #workflows = new Map<string, ValidatedWorkflow>();
  readonly #events: WorkflowEvents;

  constructor(
    readonly store: WorkflowStore,
    private readonly queue: QueueManager | null,
    bus: EventBus | null,
  ) {
    this.#events = new WorkflowEvents(bus);
  }

  /** The queue, when dispatching is possible (CLI/HTTP surface checks). */
  get canDispatch(): boolean {
    return this.queue !== null;
  }

  /** Registry view handed to the runner. */
  resolve(name: string, version: number): ValidatedWorkflow | undefined {
    const entry = this.#workflows.get(name);
    return entry && entry.workflow.version === version ? entry : undefined;
  }

  /** Registers workflow (or standalone pipeline) definitions; duplicates fail at boot. */
  register(...definitions: readonly (WorkflowDefinition | PipelineDefinition)[]): void {
    for (const definition of definitions) {
      const validated = 'input' in definition ? validateWorkflow(definition) : this.#wrapPipeline(definition);
      const existing = this.#workflows.get(validated.workflow.name);
      if (existing) {
        throw new FrameworkError('DUPLICATE_WORKFLOW', `Workflow "${validated.workflow.name}" is registered twice`);
      }
      this.#workflows.set(validated.workflow.name, validated);
    }
  }

  /** All registered definitions (for CLI listing). */
  definitions(): readonly ValidatedWorkflow[] {
    return [...this.#workflows.values()];
  }

  /**
   * Creates a run: validates the input, persists the node tree, transitions
   * to `running`, and enqueues the first pipeline unit. Idempotent at the
   * state-machine level: re-delivered units are absorbed by node claims.
   */
  async dispatch(
    target: string | WorkflowDefinition | PipelineDefinition,
    input: JsonObject,
    context: ExecutionContext,
    _options: WorkflowDispatchOptions = {},
  ): Promise<{ runId: string }> {
    const workflow = this.#resolveTarget(target);
    const parsed = workflow.workflow.input.safeParse(input);
    if (!parsed.success) {
      throw new FrameworkError(
        'WORKFLOW_INPUT_INVALID',
        `Invalid input for workflow "${workflow.workflow.name}": ${parsed.error.message}`,
      );
    }

    const tree = this.#buildTree(workflow);
    const { run } = await this.store.createRun(
      {
        workflow: workflow.workflow.name,
        version: workflow.workflow.version,
        input: parsed.data as JsonObject,
        // The validated input seeds the run state; action outputs merge on top.
        state: parsed.data as JsonObject,
        actor: context.actor,
        correlationId: context.correlationId,
      },
      tree,
    );
    await this.store.transitionRun(run.id, ['pending'], 'running', { started: true });
    await this.#events.emit(run.id, 'started', { workflow: run.workflow, version: run.version });

    const root = await this.store.rootNodeByIndex(run.id, 0);
    if (!root) throw new FrameworkError('WORKFLOW_INVALID', `Run ${run.id} has no root pipeline node`);
    await this.#enqueueUnit(run, root);
    return { runId: run.id };
  }

  async status(runId: string): Promise<WorkflowRunStatus | null> {
    const run = await this.store.runByIdOrPrefix(runId);
    if (!run) return null;
    const nodes = await this.store.nodesByRun(run.id);
    return { run, nodes };
  }

  async list(query: WorkflowQuery): Promise<Page<RunRecord>> {
    return this.store.listRuns(query);
  }

  /** Cancels a pending/running/waiting run; in-flight unit jobs observe the status and stop. */
  async cancel(runIdOrPrefix: string): Promise<boolean> {
    const run = await this.store.runByIdOrPrefix(runIdOrPrefix);
    if (!run) return false;
    const cancelled = await this.store.transitionRun(run.id, ['pending', 'running', 'waiting'], 'cancelled', {
      finished: true,
    });
    if (cancelled) await this.#events.emit(run.id, 'cancelled', { workflow: cancelled.workflow });
    return cancelled !== null;
  }

  /**
   * Re-runs a failed run from its failure point (default: the first failed
   * node in tree order). Completed work is kept; compensated actions stay
   * compensated — re-dispatch the workflow instead when side effects were
   * undone.
   */
  async retry(runIdOrPrefix: string, options: { fromNode?: string } = {}): Promise<boolean> {
    const run = await this.store.runByIdOrPrefix(runIdOrPrefix);
    if (!run || (run.status !== 'failed' && run.status !== 'compensated')) return false;
    const runId = run.id;

    const target = options.fromNode
      ? await this.store.findNode(runId, options.fromNode)
      : await this.store.firstFailedNode(runId);
    if (!target) return false;
    this.#requireRegistered(run.workflow, run.version);

    await this.store.resetSubtree(runId, target.id);
    await this.store.reviveAncestors(target.id);
    const restarted = await this.store.transitionRun(runId, ['failed', 'compensated'], 'running');
    if (!restarted) return false;

    const dispatchRoot = target.parentId ?? target.id;
    await this.#enqueueUnit(run, { ...target, id: dispatchRoot }, 3);
    return true;
  }

  /** Force-satisfies the wait of a parked run (empty payload) and continues it. */
  async resume(runIdOrPrefix: string): Promise<boolean> {
    const run = await this.store.runByIdOrPrefix(runIdOrPrefix);
    if (run?.status !== 'waiting') return false;
    const runId = run.id;
    const waiters = (await this.store.waitingStages()).filter((node) => node.runId === runId);
    const stage = waiters[0];
    if (!stage) return false;
    const completed = await this.store.completeWaiter(stage.id, {});
    if (!completed) return false;
    await this.store.transitionRun(runId, ['waiting'], 'running');
    await this.#events.emit(runId, 'resumed', { stage: stage.key, event: 'manual' });
    if (stage.parentId) {
      await this.#enqueueAdvance(run, stage.parentId);
    }
    return true;
  }

  /** Removes terminal runs older than the retention window; returns their count. */
  async prune(retentionDays: number): Promise<number> {
    const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60_000);
    return (await this.store.prune(cutoff)).length;
  }

  // -- internals -----------------------------------------------------------------

  #resolveTarget(target: string | WorkflowDefinition | PipelineDefinition): ValidatedWorkflow {
    if (typeof target === 'string') {
      const registered = this.#workflows.get(target);
      if (!registered) throw new FrameworkError('WORKFLOW_UNKNOWN', `Workflow "${target}" is not registered`);
      return registered;
    }
    if ('input' in target) {
      const validated = validateWorkflow(target);
      const existing = this.#workflows.get(validated.workflow.name);
      if (existing && existing.workflow.version !== validated.workflow.version) {
        throw new FrameworkError(
          'WORKFLOW_VERSION_CONFLICT',
          `Workflow "${validated.workflow.name}" is registered at v${existing.workflow.version}; dispatching v${validated.workflow.version} is ambiguous — register it or dispatch by name`,
        );
      }
      return validated;
    }
    return this.#wrapPipeline(target);
  }

  #wrapPipeline(pipeline: PipelineDefinition): ValidatedWorkflow {
    return validateWorkflow({
      name: pipeline.name,
      version: 1,
      input: passthroughObject(),
      pipelines: [pipeline],
    });
  }

  /** Flattens definitions into the persisted node tree (levels inserted by the store). */
  #buildTree(workflow: ValidatedWorkflow) {
    return workflow.pipelines.map((pipeline, pipelineIndex) => ({
      key: pipeline.name,
      path: pipeline.name,
      via: 'queue' as const,
      nodeIndex: pipelineIndex,
      stages: pipeline.stages.map((stage, stageIndex) => {
        const stagePath = `${pipeline.name}.${stage.name}`;
        return {
          key: stage.name,
          path: stagePath,
          via: stage.via ?? ('inline' as const),
          nodeIndex: stageIndex,
          maxAttempts: 1,
          waitEvent: stage.waitFor?.event ?? null,
          actions: flattenStageActions(stage, stagePath),
        };
      }),
    }));
  }

  #requireRegistered(name: string, version: number): ValidatedWorkflow {
    const workflow = this.resolve(name, version);
    if (!workflow) {
      throw new FrameworkError('WORKFLOW_DEFINITION_UNKNOWN', `Workflow "${name}" v${version} is not registered`);
    }
    return workflow;
  }

  #requireQueue(): QueueManager {
    if (!this.queue) {
      throw new FrameworkError('WORKFLOW_QUEUE_UNAVAILABLE', 'This WorkflowManager has no queue configured');
    }
    return this.queue;
  }

  #enqueueUnit(
    run: RunRecord,
    node: Pick<NodeRecord, 'id' | 'kind' | 'maxAttempts'>,
    attempts?: number,
  ): Promise<void> {
    const queue = this.#requireQueue();
    const tries = attempts ?? (node.kind === 'action' ? Math.max(1, node.maxAttempts) : 3);
    return enqueueInternalWorkflowJob(
      queue.queue,
      queue.options,
      WORKFLOW_UNIT_NAME,
      { runId: run.id, nodeId: node.id },
      { correlationId: run.correlationId, actor: run.actor ?? SYSTEM_ACTOR },
      tries,
    );
  }

  #enqueueAdvance(run: RunRecord, nodeId: string): Promise<void> {
    const queue = this.#requireQueue();
    return enqueueInternalWorkflowJob(
      queue.queue,
      queue.options,
      WORKFLOW_ADVANCE_NAME,
      { runId: run.id, nodeId },
      { correlationId: run.correlationId, actor: run.actor ?? SYSTEM_ACTOR },
      3,
    );
  }
}

function flattenStageActions(stage: StageDefinition, stagePath: string) {
  const flat: {
    key: string;
    path: string;
    via: 'inline' | 'queue';
    groupId: string | null;
    nodeIndex: number;
    maxAttempts: number;
  }[] = [];
  let index = 0;
  for (const entry of stageEntries(stage)) {
    if ('parallel' in entry) {
      const group = `g${index}`;
      for (const action of entry.actions) {
        flat.push({
          key: action.name,
          path: `${stagePath}.${action.name}`,
          via: action.via ?? 'inline',
          groupId: group,
          nodeIndex: index,
          maxAttempts: action.attempts ?? 3,
        });
        index += 1;
      }
    } else {
      flat.push({
        key: entry.name,
        path: `${stagePath}.${entry.name}`,
        via: entry.via ?? 'inline',
        groupId: null,
        nodeIndex: index,
        maxAttempts: entry.attempts ?? 3,
      });
      index += 1;
    }
  }
  return flat;
}

function passthroughObject() {
  // zod v4: accept any object unchanged, preserving extra keys.
  return z.custom<JsonObject>((value) => typeof value === 'object' && value !== null && !Array.isArray(value));
}
