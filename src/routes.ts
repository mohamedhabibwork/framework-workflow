import { EXECUTION_CONTEXT, type JsonObject } from '@framework/core';
import { authorizationGuard, type FrameworkEnv, type MountAuthorization, notFound, resolve } from '@framework/http';
import { validate } from '@framework/validation';
import { type Context, Hono } from 'hono';
import { z } from 'zod/v4';
import { WORKFLOW_MANAGER, type WorkflowManager } from './manager';
import { workflowQuerySchema } from './store';

const runIdParamSchema = z.object({ runId: z.string().uuid() }).strict();
const retryBodySchema = z.object({ fromNode: z.string().min(1).max(400).optional() }).strict();
const dispatchBodySchema = z
  .object({
    workflow: z.string().min(1).max(120),
    input: z
      .record(
        z.string(),
        z.union([
          z.string(),
          z.number(),
          z.boolean(),
          z.null(),
          z.array(z.unknown()),
          z.record(z.string(), z.unknown()),
        ]),
      )
      .optional(),
  })
  .strict();

export interface WorkflowRoutesOptions {
  /** Custom manager resolution (default: the container's WORKFLOW_MANAGER). */
  readonly manager?: (c: Context<FrameworkEnv>) => WorkflowManager;
  /**
   * Required mount-time authorization: a guard that throws an `HttpError`
   * (e.g. `forbidden()`) to deny, or the explicit `'public'` opt-out.
   * Typical admin guard: `requireAbility('workflow.admin')` from
   * `@framework/auth`.
   */
  readonly authorize: MountAuthorization;
}

/**
 * Workflow introspection and control:
 *
 * ```text
 * GET  /workflow            paginated runs (workflow/status/correlation filters)
 * GET  /workflow/:runId     one run with its node tree
 * POST /workflow/:runId/retry    re-run from the failure point
 * POST /workflow/:runId/cancel   cancel a pending/running/waiting run
 * POST /workflow/:runId/resume   force-satisfy a parked wait
 * POST /workflow/dispatch       dispatch a registered workflow by name
 * ```
 *
 * Authorization is required at mount time — pass a guard (e.g.
 * `requireAbility('workflow.admin')`) or the explicit `'public'` opt-out.
 * The guard runs before validation, so anonymous callers get 401/403 and
 * never observe schema details.
 */
export function workflowRoutes(options: WorkflowRoutesOptions): Hono<FrameworkEnv> {
  const managerFor = options.manager ?? ((c: Context<FrameworkEnv>) => resolve(c, WORKFLOW_MANAGER));
  const guard = authorizationGuard(options.authorize);

  return new Hono<FrameworkEnv>()
    .get('/', guard, validate('query', workflowQuerySchema), async (c) => {
      const page = await managerFor(c).list(c.req.valid('query'));
      return c.json(page, 200);
    })
    .post('/dispatch', guard, validate('json', dispatchBodySchema), async (c) => {
      const { workflow, input } = c.req.valid('json');
      const execution = resolve(c, EXECUTION_CONTEXT);
      const { runId } = await managerFor(c).dispatch(workflow, (input ?? {}) as JsonObject, execution);
      return c.json({ runId, status: 'dispatched' as const }, 202);
    })
    .get('/:runId', guard, validate('param', runIdParamSchema), async (c) => {
      const status = await managerFor(c).status(c.req.valid('param').runId);
      if (!status) throw notFound('Workflow run not found');
      return c.json(status, 200);
    })
    .post('/:runId/retry', guard, validate('param', runIdParamSchema), validate('json', retryBodySchema), async (c) => {
      const done = await managerFor(c).retry(c.req.valid('param').runId, { fromNode: c.req.valid('json').fromNode });
      if (!done) throw notFound('Nothing to retry: run not found or not in a failed state');
      return c.json({ status: 'retried' as const }, 202);
    })
    .post('/:runId/cancel', guard, validate('param', runIdParamSchema), async (c) => {
      const done = await managerFor(c).cancel(c.req.valid('param').runId);
      if (!done) throw notFound('Run not found or already in a terminal state');
      return c.json({ status: 'cancelled' as const }, 202);
    })
    .post('/:runId/resume', guard, validate('param', runIdParamSchema), async (c) => {
      const done = await managerFor(c).resume(c.req.valid('param').runId);
      if (!done) throw notFound('Run not found or not waiting');
      return c.json({ status: 'resumed' as const }, 202);
    });
}
