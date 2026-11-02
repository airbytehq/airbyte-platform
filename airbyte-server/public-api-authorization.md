# Public API authorization compatibility (PLAT-1513)

The active protected public surface has 88 operations with authentication enabled and 85 with no-auth configured. The three embedded operations are unavailable without authentication. Cloud replaces workspace creation and the OAuth rule; the remaining public methods are inherited from OSS.

The route inventory tests derive this list from Micronaut's served routes, including inherited methods, Cloud replacements and automatic HEAD companions. They compare every protected operation with an explicit HTTP request in `PublicApiHttpScenarios.kt`. Every protected public API route declares a shared rule. The three Config API root searches retain their existing behavior through a narrowly validated exception marker.

The contract reviewed is `oss/airbyte-api/server-api/src/main/openapi/config.yaml`, including its public operations, and `public_api.yaml` in the same directory. Request and response models, paths, methods and existing controller responses are unchanged. No SDK regeneration or API version change is needed for the supported requests described here.

## Supported flows and deliberate rejections

- Public application tokens retain their creating user's identity. Source reads have real HTTP checks for user tokens, application-owner tokens, matching workspace scopes, embedded workspace scopes and configured no-auth. Every protected operation has a supported HTTP request through its served route, real controller, shared rule and response serializer in both editions. Business dependencies return concrete fixtures. Assertions cover status and response IDs, list shapes, tokens or existing edition errors. Absent grants, correct-role grants for a foreign workspace/organization, and reader grants for write/admin actions reject before controller dependencies; SELF routes remain allowed without a grant. Separate rule tests are supplemental. The HTTP matrix also rejects workload credentials before controller dependencies, even when those credentials carry administrator labels.
- Scoped embedded tokens retain source CRUD and OAuth, plus destination creation in their workspace. Job actions and destination reads/deletes use the issuing user's saved permissions and the token's workspace scope. Embedded job lists require an explicit connection or workspace selector; an unselected list could expose jobs outside the token scope. Explicit workspace lists check every selected workspace; an omitted or empty selector keeps the current-user filtered list for user tokens. Deleted-resource selectors remain supported where the list contract exposes `includeDeleted`.
- Stream requests with both actor IDs retain the existing same-workspace requirement for workspace and organization callers. Instance administrators and configured no-auth retain their cross-workspace alternative. Connection creation requires its actors to share a workspace. Create and patch keep the existing behavior of skipping missing or foreign tags when saving the connection; deletes validate the saved relationships used by their cascade.
- Job lists preserve `connectionId` precedence over `workspaceIds`. Job actions retain reader/runner/editor distinctions, including the editor requirement for clear jobs. Tags used solely as list filters remain exact filters; they do not become a new permission requirement.
- Public workspace creation keeps the OSS default organization and organization-editor action. Cloud keeps its optional organization selector: an explicit organization or the unique organization selected by `DefaultOrgForNewWorkspace` needs organization-admin access; a single membership remains the fallback. Multiple memberships without a unique flagged default retain the existing 500 problem; a new user's implicit organization remains a SELF flow. Region selectors must be available to the selected organization.
- Permission reads and writes retain workspace and organization alternatives. GLOBAL permission GET/PATCH/DELETE retain their existing 403 in authenticated and configured no-auth modes, before business reads or writes. The old controller required `INSTANCE_ADMIN`, while saved instance grants resolve `ADMIN`; the shared rule preserves this restriction. A real `RoleResolver` baseline and OSS/Cloud HTTP tests cover both modes. SELF permission lists ignore an irrelevant organization selector. Application IDs remain exact provider strings, including a 404 for unknown non-UUID IDs. Nested group-permission deletion requires the saved group parent; member removal remains an idempotent exact filter.
- Missing, unavailable and wrong-kind definitions retain 404 with the definition ID and `STANDARD_SOURCE_DEFINITION` / `STANDARD_DESTINATION_DEFINITION` in the problem data. Saved-tag 404s and missing definition/actor IDs used during creation also retain their existing Config API resource type and ID. Group, member and nested-permission 404 resource payloads and application-not-found detail were checked too. Definition reads check the requested workspace and saved definition availability, including grants shared from another workspace. Writes retain custom-definition and source/destination editor differences. Declarative writes require the saved builder association. Cloud's existing rejection of non-declarative definition create/update remains in the controller.
- OSS OAuth retains its static 501 response after the original workspace action. Cloud additionally resolves the selected source definition before starting its OAuth flow. Health, documentation, token issuance and OAuth callback retain their explicit public/login declarations.

Intentional security rejections are wrong caller types (403), wrong actions or workspace scopes (403), foreign actor/region relationships (400 or 403), unavailable definitions and mismatched nested group parents (404), missing saved authorization resources (404), and failed authorization lookups (500). Existing business errors still come from their original controller/service path. The endpoint tests check representative missing rows, I/O failures and relationship mismatches before controller work. The per-route response matrix preserves OSS OAuth/workspace OAuth 501 and Cloud non-declarative definition create/update 400; those unsupported actions do not have successful responses in that edition.

Affected client categories are the existing public application/SDK callers, scoped user tokens, and embedded source callers. Their published payloads are unchanged. The repository does not identify individual production applications or supply a traffic audit; no claim about measured production-client usage is made here. Internal worker credentials sent to public routes are deliberately rejected. PLAT-1506 telemetry is outside this change.

## Per-route checks

“HTTP response” checks the served route, controller and serializer with explicit business dependency fixtures, including the expected status and response IDs or collection/token shape. “Action/scope denial” checks absent grants, foreign scopes and insufficient actions for 403 before controller dependencies. “SELF” needs no resource grant. “Wrong caller” sends workload credentials and checks rejection before controller dependencies. This table summarizes the HTTP scenarios; `assertPublicRouteHttpCoverage` checks the scenarios against the served inventory. Automatic HEAD companions use the same declaration.

| Operation | Declared rule | Editions | Compatibility checks |
| --- | --- | --- | --- |
| `GET /api/public/v1/applications` | `PublicCurrentUserRule` | Both | HTTP response, SELF, wrong caller |
| `POST /api/public/v1/applications` | `PublicCurrentUserRule` | Both | HTTP response, SELF, wrong caller |
| `DELETE /api/public/v1/applications/{applicationId}` | `PublicOwnedApplicationRule` | Both | HTTP response, SELF, wrong caller |
| `GET /api/public/v1/applications/{applicationId}` | `PublicOwnedApplicationRule` | Both | HTTP response, SELF, wrong caller |
| `GET /api/public/v1/connections` | `PublicConnectionListRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/connections` | `PublicConnectionCreateRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/connections/{connectionId}` | `PublicConnectionDeleteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/connections/{connectionId}` | `PublicConnectionReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PATCH /api/public/v1/connections/{connectionId}` | `PublicConnectionPatchRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/connector_definitions` | `PublicConnectorDefinitionListRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/dataplanes` | `PublicDataplaneListRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/dataplanes` | `PublicDataplaneCreateRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/dataplanes/{dataplaneId}` | `PublicDataplaneAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/dataplanes/{dataplaneId}` | `PublicDataplaneAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PATCH /api/public/v1/dataplanes/{dataplaneId}` | `PublicDataplaneAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/destinations` | `PublicWorkspaceListRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/destinations` | `PublicDestinationCreateRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/destinations/{destinationId}` | `PublicDestinationDeleteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/destinations/{destinationId}` | `PublicDestinationReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PATCH /api/public/v1/destinations/{destinationId}` | `PublicDestinationWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PUT /api/public/v1/destinations/{destinationId}` | `PublicDestinationWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/embedded/organizations` | `PublicCurrentUserRule` | Both | HTTP response, SELF, wrong caller |
| `POST /api/public/v1/embedded/scoped_token` | `PublicEmbeddedTokenRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/embedded/widget_token` | `PublicOrganizationBodyAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/groups` | `PublicGroupListRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/groups` | `PublicOrganizationBodyAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/groups/{groupId}` | `PublicGroupSavedRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/groups/{groupId}` | `PublicGroupSavedRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PATCH /api/public/v1/groups/{groupId}` | `PublicGroupSavedRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/groups/{groupId}/members` | `PublicGroupSavedRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/groups/{groupId}/members` | `PublicGroupMemberAddRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/groups/{groupId}/members/{userId}` | `PublicGroupSavedRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/groups/{groupId}/permissions` | `PublicGroupSavedRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/groups/{groupId}/permissions` | `PublicGroupPermissionCreateRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/groups/{groupId}/permissions/{permissionId}` | `PublicGroupPermissionDeleteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/jobs` | `PublicJobListRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/jobs` | `PublicJobCreateRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/jobs/{jobId}` | `PublicJobCancelRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/jobs/{jobId}` | `PublicJobReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/organizations` | `PublicCurrentUserRule` | Both | HTTP response, SELF, wrong caller |
| `PUT /api/public/v1/organizations/{organizationId}/oauthCredentials` | `PublicOrganizationAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/organizations/{organizationId}/oauthCredentials/{actorType}/{name}` | `PublicOrganizationAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/permissions` | `PublicPermissionListRule` | Both | HTTP response, SELF, wrong caller |
| `POST /api/public/v1/permissions` | `PublicPermissionCreateRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/permissions/{permissionId}` | `PublicPermissionWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/permissions/{permissionId}` | `PublicPermissionReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PATCH /api/public/v1/permissions/{permissionId}` | `PublicPermissionWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/regions` | `PublicOrganizationAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/regions` | `PublicOrganizationBodyAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/regions/{regionId}` | `PublicRegionAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/regions/{regionId}` | `PublicRegionAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PATCH /api/public/v1/regions/{regionId}` | `PublicRegionAdminRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/sources` | `PublicWorkspaceListRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/sources` | `PublicSourceCreateRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/sources/initiateOAuth` | `PublicSourceOAuthRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/sources/{sourceId}` | `PublicSourceDeleteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/sources/{sourceId}` | `PublicSourceReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PATCH /api/public/v1/sources/{sourceId}` | `PublicSourceWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PUT /api/public/v1/sources/{sourceId}` | `PublicSourceWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/streams` | `PublicStreamsRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/tags` | `PublicWorkspaceListRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/tags` | `PublicTagCreateRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/tags/{tagId}` | `PublicTagWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/tags/{tagId}` | `PublicTagReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PATCH /api/public/v1/tags/{tagId}` | `PublicTagWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/users` | `PublicOrganizationMemberRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/workspaces` | `PublicWorkspaceListRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/workspaces` | `PublicWorkspaceCreateRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/workspaces/{workspaceId}` | `PublicWorkspaceWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/workspaces/{workspaceId}` | `PublicWorkspaceReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PATCH /api/public/v1/workspaces/{workspaceId}` | `PublicWorkspaceUpdateRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/workspaces/{workspaceId}/definitions/declarative_sources` | `PublicWorkspaceReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/workspaces/{workspaceId}/definitions/declarative_sources` | `PublicDeclarativeDefinitionWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/workspaces/{workspaceId}/definitions/declarative_sources/{definitionId}` | `PublicDeclarativeDefinitionWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/workspaces/{workspaceId}/definitions/declarative_sources/{definitionId}` | `PublicDeclarativeDefinitionReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PUT /api/public/v1/workspaces/{workspaceId}/definitions/declarative_sources/{definitionId}` | `PublicDeclarativeDefinitionWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/workspaces/{workspaceId}/definitions/destinations` | `PublicWorkspaceReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/workspaces/{workspaceId}/definitions/destinations` | `PublicDestinationDefinitionWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/workspaces/{workspaceId}/definitions/destinations/{definitionId}` | `PublicDestinationDefinitionWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/workspaces/{workspaceId}/definitions/destinations/{definitionId}` | `PublicDestinationDefinitionReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PUT /api/public/v1/workspaces/{workspaceId}/definitions/destinations/{definitionId}` | `PublicDestinationDefinitionWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/workspaces/{workspaceId}/definitions/sources` | `PublicWorkspaceReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `POST /api/public/v1/workspaces/{workspaceId}/definitions/sources` | `PublicSourceDefinitionWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/workspaces/{workspaceId}/definitions/sources/{definitionId}` | `PublicSourceDefinitionWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `GET /api/public/v1/workspaces/{workspaceId}/definitions/sources/{definitionId}` | `PublicSourceDefinitionReadRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PUT /api/public/v1/workspaces/{workspaceId}/definitions/sources/{definitionId}` | `PublicSourceDefinitionWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `PUT /api/public/v1/workspaces/{workspaceId}/oauthCredentials` | `PublicWorkspaceWriteRule` | Both | HTTP response, action/scope denial, wrong caller |
| `DELETE /api/public/v1/workspaces/{workspaceId}/oauthCredentials/{actorType}/{name}` | `PublicWorkspaceWriteRule` | Both | HTTP response, action/scope denial, wrong caller |

Cloud's `POST /api/public/v1/workspaces` declares `PublicCloudWorkspaceCreateRule`. Cloud's `PublicCloudSourceOAuthRule` replaces the OSS OAuth rule while keeping the same route declaration and request/response contract.
