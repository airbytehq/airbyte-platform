# Authorization verification and rollback (PLAT-1515)

This work is stacked on PLAT-1513. It removes duplicate controller permission checks after shared rules cover their action, caller and saved resource relationships. Micronaut still validates tokens and requires authentication at entry. Public/login declarations, SCIM authentication, header stripping, input checks, identity checks and scoped-token restrictions remain in place.

Production rollout and signoff are pending. PLAT-1506 metrics are deferred by request; this branch does not satisfy the ticket's production metrics criterion and must not be used to mark PLAT-1515 Done.

## Inventory and compatibility

`RouteAccessCoverageTest` and `CloudRouteAccessCoverageTest` derive the actual served routes in authenticated and no-auth configurations, including inherited methods, Cloud replacements and HEAD routes. Reports are written under each module's `build/reports/route-access/`. The Config operation/rule inventory is frozen in `src/testFixtures/resources/auth/remaining-config-route-rules.tsv`; the public route matrix and compatibility decisions are in [public-api-authorization.md](public-api-authorization.md).

Every in-scope protected route must declare a concrete shared rule. Startup rejects missing or invalid declarations. The runtime filter only admits managed routes that passed startup validation, including after an earlier security `ALLOWED`. The interceptor always runs the shared rule before the controller. Lookup failures never fall back to the old authority.

The three Config searches, `POST /api/v1/sources/search`, `/destinations/search` and `/connections/search`, retain their existing behavior in both editions. Their search-scoping project was canceled. Each implementation declares `@ConfigSearchAccessException`. Startup permits this marker only on those three POST paths, with authentication and without a shared rule. There is no general authorization bypass or migration status. SCIM and the separate workload API retain their own authorization boundaries.

The unused `@RequiresIntent` annotation and `IntentSecurityRule` have been removed. Generated `Intent` role bundles remain in use. `RoleResolver` remains part of the shared permission calculation and other service/workload callers. Workspace bootstrap retains its service-level check because user provisioning calls it outside the migrated controller path.

Supported public request, response and error shapes remain unchanged; no OpenAPI or SDK changes are needed. Missing/invalid authentication keeps 401, malformed input/relationships keep their route-specific 400/422 response, insufficient actions/callers keep 403, and missing resources keep their existing 404 shape. Fusion search continues to hide denied actors behind its 404 response. Required lookup failures stop controller work and keep their existing dependency-error response. The accepted five-minute workspace-to-organization cache expiry and local invalidation are unchanged.

Public GLOBAL permission GET/PATCH/DELETE keep their existing 403 even when authentication is disabled. The shared permission rule preserves this restriction before business reads or writes; ordinary workspace and organization permission flows remain available. The baseline uses real role resolution, and HTTP tests cover both editions with and without authentication.

## Safe rollback

Keep the shared declarations, rules, interceptor and runtime declaration filter when rolling back controller cleanup. Restore an old controller check only as an additional restriction. Do not remove an enforced route's rule, bypass a failed rule or re-enable a previously unguarded operation. If a route cannot safely serve traffic with its shared rule, disable that operation until fixed.

| Enforced route family | Rollback requirement |
| --- | --- |
| Config sources/destinations, actor definitions, OAuth, catalogs and builder operations | Keep saved actor/workspace ownership, definition access, parent checks and caller/action checks before reads, credentials or writes. Disable the affected operation if these checks cannot run. |
| Config connections, events, state, operations, jobs and attempts | Keep saved connection/actor/event relationships and worker assignment checks before reads or writes. Never restore the event or worker authorization gaps. |
| Config workspace, organization, membership, permissions, groups, secrets, SSO and domain verification | Keep explicit tenant ownership, SELF/GLOBAL restrictions, SCIM-managed membership safeguards and secret-access checks. Disable affected writes or credential operations if necessary. |
| Config billing, private links, dataplanes, commands, audit, diagnostics and internal/admin operations | Keep the declared caller/action rule and saved organization/resource relationship checks. Preserve worker and service-account identity binding. |
| Public API, embedded and application/token flows | Keep the shared caller/action/resource rules, existing public/login entry points, owner identity and scoped-token restrictions. Preserve published request and error shapes; disable an affected protected operation rather than bypassing authorization. |
| Cloud replacements, Fusion execution/search/enablement/backfill and worker resolution | Keep the served replacement's shared rule, saved tenant/actor/connection relationships, signed caller identity, body/header guards and worker assignment. Disable the affected Cloud operation if any required check is unavailable. |

This change needs no schema, infrastructure, token-store or data rollback. Validate the affected route family in OSS and Cloud after rollback and retain denial-before-business tests. CI remains responsible for broad repository checks; local verification is scoped to the changed authorization paths.
