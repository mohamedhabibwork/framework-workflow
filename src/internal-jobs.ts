import type { Actor, JsonObject } from '@framework/core';
import type { Queue } from 'bullmq';
import { z } from 'zod/v4';

/**
 * Internal job contracts of the workflow engine. The WorkflowWorker wraps
 * these payloads into JobDefinitions with runner closures; applications never
 * register them manually. Dispatch uses the same BullMQ envelope
 * (`v`, `payload`, `actor`, `correlationId`) as `QueueManager.dispatch`.
 */
export const WORKFLOW_UNIT_NAME = '__workflow.unit';
export const WORKFLOW_ADVANCE_NAME = '__workflow.advance';
export const WORKFLOW_ENVELOPE_VERSION = 1;

export const workflowUnitPayloadSchema = z.object({ runId: z.string().uuid(), nodeId: z.string().uuid() });
export const workflowAdvancePayloadSchema = z.object({ runId: z.string().uuid(), nodeId: z.string().uuid() });

export type WorkflowUnitPayload = z.output<typeof workflowUnitPayloadSchema>;
export type WorkflowAdvancePayload = z.output<typeof workflowAdvancePayloadSchema>;

/** Adds one internal workflow job to the queue. */
export async function enqueueInternalWorkflowJob(
  queue: Queue,
  keep: { readonly keepCompleted?: number; readonly keepFailed?: number },
  name: string,
  payload: JsonObject,
  identity: { readonly correlationId: string; readonly actor: Actor },
  attempts: number,
): Promise<void> {
  await queue.add(
    name,
    { v: WORKFLOW_ENVELOPE_VERSION, payload, actor: identity.actor, correlationId: identity.correlationId },
    {
      attempts,
      backoff: { type: 'exponential', delay: 500 },
      removeOnComplete: { count: keep.keepCompleted ?? 1_000 },
      removeOnFail: { count: keep.keepFailed ?? 5_000 },
    },
  );
}
