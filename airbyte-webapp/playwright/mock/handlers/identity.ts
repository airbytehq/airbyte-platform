import type { HandlerMap } from "./handler";
import type { Store } from "../store";
import type { InstanceConfigurationResponse } from "@src/core/api/types/AirbyteClient";

import { notFound } from "./handler";

/** True when a request names a workspace that the scenario does not have. Handlers answer it with a 404. */
export function isOtherWorkspace(store: Store, workspaceId: string) {
  return workspaceId !== store.identity.workspace.workspaceId;
}

/** The instance configuration. As on the server, initialSetupComplete is the flag of the default workspace. */
function instanceConfiguration(store: Store): InstanceConfigurationResponse {
  return {
    edition: "community",
    version: "dev",
    auth: { mode: "none" },
    airbyteUrl: "http://localhost:8000",
    initialSetupComplete: store.identity.workspace.initialSetupComplete,
    defaultUserId: store.identity.user.userId,
    defaultOrganizationId: store.identity.organization.organizationId,
    defaultOrganizationEmail: store.identity.organization.email,
    defaultWorkspaceId: store.identity.workspace.workspaceId,
    trackingStrategy: "logging",
  };
}

/**
 * The calls every page makes on startup, and the setup page. The identity is OSS community edition, auth mode "none",
 * one organization, one workspace, and one user with organization_admin permission.
 */
export const identityHandlers: HandlerMap = {
  getWebappConfig: () => ({ version: "dev", edition: "community" }),
  getHealthCheck: () => ({ available: true }),
  getInstanceConfiguration: ({ store }) => instanceConfiguration(store),

  /**
   * Saves the setup form as the server does (InstanceConfigurationHandler.kt): the email goes to the organization, the
   * user and the workspace, and the setup flags go to the workspace.
   */
  setupInstanceConfiguration: ({ body, store }) => {
    store.updateIdentity({
      user: { email: body.email, ...(body.userName ? { name: body.userName } : {}) },
      organization: {
        email: body.email,
        ...(body.organizationName ? { organizationName: body.organizationName } : {}),
      },
      workspace: {
        email: body.email,
        anonymousDataCollection: body.anonymousDataCollection,
        initialSetupComplete: body.initialSetupComplete,
        displaySetupWizard: body.displaySetupWizard,
      },
    });
    return instanceConfiguration(store);
  },

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
  listWorkspacesByUser: ({ body, store }) => {
    if (body.userId !== store.identity.user.userId) {
      return notFound();
    }
    if (body.nameContains) {
      throw new Error("The mock does not support nameContains on the workspace list");
    }
    // The scenario has one workspace, so only the first page lists it.
    const isFirstPage = !body.pagination?.rowOffset;
    return { workspaces: isFirstPage ? [store.identity.workspace] : [] };
  },
  listWorkspacesInOrganization: ({ body, store }) =>
    body.organizationId === store.identity.organization.organizationId
      ? { workspaces: [store.identity.workspace] }
      : notFound(),
  listDataplaneGroups: () => ({ dataplaneGroups: [] }),
  listTags: () => [],
};
