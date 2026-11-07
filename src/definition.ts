import { FrameworkError, type JsonObject, type JsonValue } from '@framework/core';
import type {
  ActionDefinition,
  AnyActionDefinition,
  ParallelGroup,
  PipelineDefinition,
  StageAction,
  StageDefinition,
  WorkflowContext,
  WorkflowDefinition,
  WorkflowState,
} from './types';

/** Declares an atomic, compensable unit of work (inline or queued). */
export const defineAction = <TInput extends JsonObject, TResult extends JsonValue>(
  definition: ActionDefinition<TInput, TResult>,
): ActionDefinition<TInput, TResult> => definition;

/** Declares a checkpoint boundary gating and sequencing actions. */
export const defineStage = (definition: StageDefinition): StageDefinition => definition;

/** Declares an ordered sequence of stages; dispatchable on its own. */
export const definePipeline = (definition: PipelineDefinition): PipelineDefinition => definition;

/** Declares a versioned durable workflow over one or more pipelines. */
export const defineWorkflow = <TInput extends JsonObject>(
  definition: WorkflowDefinition<TInput>,
): WorkflowDefinition<TInput> => definition;

/** Marks actions that run concurrently; the stage resumes once all are terminal. */
export const parallel = (actions: readonly AnyActionDefinition[]): ParallelGroup => ({ parallel: true, actions });

/** Definition names seed node paths and BullMQ keys: no dots, lowercase. */
const NAME_PATTERN = /^[a-z][a-z0-9_-]{0,127}$/;

/** Normalizes `actions` into a list (a bare parallel group wraps itself). */
export function stageEntries(stage: StageDefinition): readonly StageAction[] {
  const actions = stage.actions;
  if (!actions) return [];
  // Array.isArray cannot exclude `readonly T[]` from a union, hence the casts.
  return Array.isArray(actions) ? (actions as readonly StageAction[]) : [actions as StageAction];
}

/** Flattens a stage's actions (parallel groups collapse into order). */
export function flattenActions(stage: StageDefinition): readonly AnyActionDefinition[] {
  let entries: readonly StageAction[] = stageEntries(stage);
  if (entries.length === 0) {
    // Convenience default: the stage's `run` becomes a single implicit action.
    entries = [
      {
        name: `${stage.name}#run`,
        run: (state: WorkflowState, context: WorkflowContext) => stage.run?.(state, context) ?? null,
      } as unknown as AnyActionDefinition,
    ];
  }
  return entries.flatMap((entry: StageAction): AnyActionDefinition[] =>
    'parallel' in entry ? [...entry.actions] : [entry as AnyActionDefinition],
  );
}

/** Returns the group id each flattened action belongs to (null = sequential). */
export function flattenGroups(stage: StageDefinition): readonly (string | null)[] {
  return stageEntries(stage).flatMap((entry, i) =>
    'parallel' in entry ? Array.from({ length: entry.actions.length }, () => `g${i}`) : [null],
  );
}

export interface ValidatedWorkflow {
  readonly workflow: WorkflowDefinition;
  /** Explicit pipelines, or the single implicit pipeline built from `stages`. */
  readonly pipelines: readonly PipelineDefinition[];
}

/**
 * Structural validation at registration time: names, version, non-empty
 * pipelines. Fails fast with stable codes, like the job and middleware
 * registries.
 */
export function validateWorkflow(definition: WorkflowDefinition): ValidatedWorkflow {
  if (!NAME_PATTERN.test(definition.name)) {
    throw new FrameworkError(
      'WORKFLOW_INVALID',
      `Workflow name "${definition.name}" must match ${NAME_PATTERN.source}`,
    );
  }
  if (!Number.isInteger(definition.version) || definition.version < 1) {
    throw new FrameworkError('WORKFLOW_INVALID', `Workflow "${definition.name}" needs a positive integer version`);
  }
  const pipelines =
    definition.pipelines ?? (definition.stages ? [{ name: definition.name, stages: definition.stages }] : []);
  if (pipelines.length === 0) {
    throw new FrameworkError('WORKFLOW_INVALID', `Workflow "${definition.name}" declares no pipelines or stages`);
  }
  for (const pipeline of pipelines) {
    if (!NAME_PATTERN.test(pipeline.name)) {
      throw new FrameworkError(
        'WORKFLOW_INVALID',
        `Pipeline name "${pipeline.name}" must match ${NAME_PATTERN.source}`,
      );
    }
    if (pipeline.stages.length === 0) {
      throw new FrameworkError('WORKFLOW_INVALID', `Pipeline "${pipeline.name}" declares no stages`);
    }
    for (const stage of pipeline.stages) {
      validateStage(pipeline.name, stage);
    }
  }
  return { workflow: definition, pipelines };
}

function validateStage(pipelineName: string, stage: StageDefinition): void {
  if (!NAME_PATTERN.test(stage.name)) {
    throw new FrameworkError('WORKFLOW_INVALID', `Stage name "${stage.name}" must match ${NAME_PATTERN.source}`);
  }
  if (!stage.actions && !stage.run && !stage.waitFor) {
    throw new FrameworkError(
      'WORKFLOW_INVALID',
      `Stage "${pipelineName}.${stage.name}" needs actions, a default run, or a waitFor`,
    );
  }
  const actions = flattenActions(stage);
  const names = new Set<string>();
  for (const action of actions) {
    const key = action.name;
    if (names.has(key)) {
      throw new FrameworkError('WORKFLOW_INVALID', `Duplicate action name "${key}" in stage "${stage.name}"`);
    }
    if (!NAME_PATTERN.test(key) && !key.startsWith(`${stage.name}#`)) {
      throw new FrameworkError('WORKFLOW_INVALID', `Action name "${key}" must match ${NAME_PATTERN.source}`);
    }
    names.add(key);
  }
}
