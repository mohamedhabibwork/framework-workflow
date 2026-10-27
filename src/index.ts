// Public API of @framework/workflow.
// Convention: every interface/type lives in `./types`, every constant in
// `./constants`; runtime values (classes, functions) stay in their modules.

export {
  defineAction,
  definePipeline,
  defineStage,
  defineWorkflow,
  flattenActions,
  parallel,
  validateWorkflow,
} from './definition';
export { WorkflowEngine, type WorkflowEngineOptions } from './engine';
// Every interface and type alias, in one import point.
export type { WorkflowEventVerb } from './events';
export { WorkflowEvents } from './events';
export {
  WORKFLOW_ADVANCE_NAME,
  WORKFLOW_UNIT_NAME,
  workflowAdvancePayloadSchema,
  workflowUnitPayloadSchema,
} from './internal-jobs';
export { WORKFLOW_MANAGER, WorkflowManager } from './manager';
export { WORKFLOW_STORE, workflowProvider } from './provider';
export { workflowRoutes } from './routes';
export { type RunnerOptions, type WorkflowRegistryView, WorkflowRunner } from './runner';
export { workflowNodes, workflowRuns } from './schema';
export type { WorkflowQuery } from './store';
export { WorkflowStore, workflowQuerySchema } from './store';
export type {
  ActionDefinition,
  AnyActionDefinition,
  ExecutionVia,
  NodeKind,
  NodeRecord,
  NodeStatus,
  ParallelGroup,
  PipelineDefinition,
  RunRecord,
  RunStatus,
  StageAction,
  StageDefinition,
  WorkflowContext,
  WorkflowDefinition,
  WorkflowRunStatus,
  WorkflowState,
} from './types';
