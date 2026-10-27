import { currentExecution, type JsonObject } from '@framework/core';
import { createDomainEvent, type DomainEvent, type EventBus } from '@framework/events';

export type WorkflowEventVerb =
  | 'started'
  | 'stage.completed'
  | 'stage.failed'
  | 'action.completed'
  | 'action.failed'
  | 'action.skipped'
  | 'action.compensated'
  | 'waiting'
  | 'resumed'
  | 'compensating'
  | 'completed'
  | 'compensated'
  | 'failed'
  | 'cancelled';

/**
 * Publishes run progress onto the event bus: once on the aggregate `workflow`
 * channel and once on the per-run `workflow.run.{runId}` channel (what
 * WebSocket dashboards subscribe to). Missing bus is a no-op (workflows run
 * without events).
 */
export class WorkflowEvents {
  constructor(private readonly bus: EventBus | null) {}

  emit(runId: string, verb: WorkflowEventVerb, extra: JsonObject = {}): Promise<void> {
    if (!this.bus) return Promise.resolve();
    const execution = currentExecution();
    const aggregate = createDomainEvent('workflow', verb, { runId, ...extra }, execution);
    const perRun = createDomainEvent(`workflow.run.${runId}`, verb, { runId, ...extra }, execution);
    return this.publishAll([aggregate, perRun]);
  }

  private async publishAll(events: readonly DomainEvent[]): Promise<void> {
    const bus = this.bus;
    if (!bus) return;
    const results = await Promise.allSettled(events.map((event) => bus.publish(event)));
    for (const result of results) {
      if (result.status === 'rejected') {
        console.error('[workflow] failed to publish event', result.reason);
      }
    }
  }
}
