import type { HandlerMap } from "./handler";
import type { Store } from "../store";

import { notFound } from "./handler";

/** True when a request names a workspace that the scenario does not have. Handlers answer it with a 404. */
export function isOtherWorkspace(store: Store, workspaceId: string) {
  return workspaceId !== store.identity.workspace.workspaceId;
}

/**
 * The calls every page makes on startup. The identity is fixed: OSS community edition, auth mode "none",
 * one organization, one workspace, and one user with organization_admin permission.
 */
export const identityHandlers: HandlerMap = {
  getWebappConfig: () => ({ version: "dev", edition: "community" }),
  getHealthCheck: () => ({ available: true }),
  getInstanceConfiguration: ({ store }) => ({
    edition: "community",
    version: "dev",
    auth: { mode: "none" },
    airbyteUrl: "http://localhost:8000",
    initialSetupComplete: true,
    defaultUserId: store.identity.user.userId,
    defaultOrganizationId: store.identity.organization.organizationId,
    defaultOrganizationEmail: store.identity.user.email,
    defaultWorkspaceId: store.identity.workspace.workspaceId,
    trackingStrategy: "logging",
  }),
  webBackendCheckUpdates: () => ({ sourceDefinitions: 0, destinationDefinitions: 0 }),

  getUser: ({ body, store }) => (body.userId === store.identity.user.userId ? store.identity.user : notFound()),
  listPermissionsByUser: ({ body, store }) =>
    body.userId === store.identity.user.userId
      ? {
          permissions: [
            {
              permissionId: "f0000000-0000-4000-8000-000000000001",
              permissionType: "organization_admin",
              userId: store.identity.user.userId,
              organizationId: store.identity.organization.organizationId,
            },
          ],
        }
      : notFound(),

  getOrganization: ({ body, store }) =>
    body.organizationId === store.identity.organization.organizationId ? store.identity.organization : notFound(),
  getOrgInfo: ({ body, store }) =>
    body.organizationId === store.identity.organization.organizationId
      ? { ...store.identity.organization, sso: false, scim: false }
      : notFound(),
  listOrganizationsByUser: ({ store }) => ({ organizations: [store.identity.organization] }),
  listOrganizationSummaries: ({ store }) => ({
    organizationSummaries: [
      { organization: store.identity.organization, workspaces: [store.identity.workspace], memberCount: 1 },
    ],
  }),

  getWorkspace: ({ body, store }) =>
    isOtherWorkspace(store, body.workspaceId) ? notFound() : store.identity.workspace,
  listWorkspacesInOrganization: ({ body, store }) =>
    body.organizationId === store.identity.organization.organizationId
      ? { workspaces: [store.identity.workspace] }
      : notFound(),
  listDataplaneGroups: () => ({ dataplaneGroups: [] }),
  listTags: () => [],
};
