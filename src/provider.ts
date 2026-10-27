import { defineProvider, token } from '@framework/core';
import { DATABASE } from '@framework/database';
import { EVENT_BUS } from '@framework/events';
import { QUEUE } from '@framework/queue';
import { WORKFLOW_MANAGER, WorkflowManager } from './manager';
import { WorkflowStore } from './store';

export const WORKFLOW_STORE = token<WorkflowStore>('workflow.store');

/**
 * Binds the workflow engine into the container. Factories are lazy, so the
 * provider registers safely in any order; resolution requires the app to
 * provide `DATABASE` and `QUEUE`, and optionally `EVENT_BUS`.
 */
export const workflowProvider = defineProvider({
  name: 'workflow',
  register(context) {
    const { container } = context;
    container.singleton(WORKFLOW_STORE, () => new WorkflowStore(container.resolve(DATABASE).db));
    container.singleton(
      WORKFLOW_MANAGER,
      () =>
        new WorkflowManager(
          container.resolve(WORKFLOW_STORE),
          container.resolve(QUEUE),
          container.has(EVENT_BUS) ? container.resolve(EVENT_BUS) : null,
        ),
    );
  },
});
