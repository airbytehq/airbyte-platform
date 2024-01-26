import { intentToRbacQuery } from "./intents";
import { useRbac } from "./rbac";
import { RbacQuery, RbacQueryWithoutResourceId, RbacResource } from "./rbacPermissionsQuery";

type IntentToRbacQuery = typeof intentToRbacQuery;
// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface AllIntents extends IntentToRbacQuery {}
export type Intent = keyof AllIntents;

interface OrganizationIntentMeta {
  organizationId: string | undefined;
}
interface WorkspaceIntentMeta {
  workspaceId: string;
}

// Utility types to enforce shape of any meta object; i.e. organization-focused intents shouldn't receive workspaceIds
// provides proper autocomplete hinting for `meta` and to future proof changes to Intent:Query mapping,
// e.g. changing Intent X from resource:"WORKSPACE" to resource:"ORGANIZATION" should catch instances of invalid meta
type MapIntentToResource<I extends Intent> = AllIntents[I]["resourceType"];
type IntentMetaForResource<R extends RbacResource> = R extends "INSTANCE"
  ? Record<string, never>
  : R extends "ORGANIZATION"
  ? OrganizationIntentMeta
  : R extends "WORKSPACE"
  ? WorkspaceIntentMeta
  : never;

/*
Given the React context + overrides provided in optional `details`,
determine if the user has the required permissions to perform the given `intent`.
*/
export const useIntent = <I extends Intent>(intent: I, meta: IntentMetaForResource<MapIntentToResource<I>>) => {
  let query: RbacQuery | RbacQueryWithoutResourceId = intentToRbacQuery[intent as keyof IntentToRbacQuery];
  if (isOrganizationIntentMeta(query.resourceType, meta)) {
    query = { ...query, resourceId: meta.organizationId };
  } else if (isWorkspaceIntentMeta(query.resourceType, meta)) {
    query = { ...query, resourceId: meta.workspaceId };
  }

  return useRbac(query);
};

function isOrganizationIntentMeta(
  resource: RbacResource,
  _meta: OrganizationIntentMeta | WorkspaceIntentMeta
): _meta is OrganizationIntentMeta {
  return resource === "ORGANIZATION";
}

function isWorkspaceIntentMeta(
  resource: RbacResource,
  _meta: OrganizationIntentMeta | WorkspaceIntentMeta
): _meta is WorkspaceIntentMeta {
  return resource === "WORKSPACE";
}
