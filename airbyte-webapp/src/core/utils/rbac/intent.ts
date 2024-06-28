import { intentToRbacQuery } from "./intents";
import { useRbac } from "./rbac";
import { RbacQuery, RbacQueryWithoutResourceId, RbacResource } from "./rbacPermissionsQuery";

type IntentToRbacQuery = typeof intentToRbacQuery;
// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface AllIntents extends IntentToRbacQuery {}
export type Intent = keyof AllIntents;

interface ResourceIntentMeta extends Record<string, never> {}

interface OrganizationIntentMeta {
  organizationId: string | undefined;
}
interface WorkspaceIntentMeta {
  workspaceId: string;
}

type IntentMeta = OrganizationIntentMeta | WorkspaceIntentMeta | ResourceIntentMeta;

// Utility types to enforce shape of any meta object; i.e. organization-focused intents shouldn't receive workspaceIds
// provides proper autocomplete hinting for `meta` and to future proof changes to Intent:Query mapping,
// e.g. changing Intent X from resource:"WORKSPACE" to resource:"ORGANIZATION" should catch instances of invalid meta
type MapIntentToResource<I extends Intent> = AllIntents[I] extends ReadonlyArray<{ resourceType: RbacResource }>
  ? AllIntents[I][number]["resourceType"]
  : AllIntents[I] extends { resourceType: RbacResource }
  ? // extra extends is silly but TS errors otherwise; while still finding the correct type 🤷
    AllIntents[I]["resourceType"]
  : never;

// from https://stackoverflow.com/questions/50374908/transform-union-type-to-intersection-type
type UnionToIntersection<U> = (U extends unknown ? (x: U) => void : never) extends (x: infer I) => void ? I : never;

type IntentMetaForResource<R extends RbacResource> = R extends "INSTANCE"
  ? ResourceIntentMeta
  : R extends "ORGANIZATION"
  ? OrganizationIntentMeta
  : R extends "WORKSPACE"
  ? WorkspaceIntentMeta
  : never;

/*
Given the React context + overrides provided in optional `details`,
determine if the user has the required permissions to perform the given `intent`.
*/
export const useIntent = <I extends Intent>(
  intent: I,
  _meta: UnionToIntersection<IntentMetaForResource<MapIntentToResource<I>>>
) => {
  const match: RbacQuery | RbacQueryWithoutResourceId | ReadonlyArray<RbacQuery | RbacQueryWithoutResourceId> =
    intentToRbacQuery[intent as keyof IntentToRbacQuery];

  const meta: IntentMeta = _meta as unknown as IntentMeta;

  const queries = (Array.isArray(match) ? match : [match]).map<RbacQuery>((query) => {
    if (isOrganizationIntentMeta(query.resourceType, meta)) {
      return { ...query, resourceId: meta.organizationId };
    } else if (isWorkspaceIntentMeta(query.resourceType, meta)) {
      return { ...query, resourceId: meta.workspaceId };
    }
    return query;
  });

  return useRbac(queries);
};

function isOrganizationIntentMeta(resource: RbacResource, _meta: IntentMeta): _meta is OrganizationIntentMeta {
  return resource === "ORGANIZATION";
}

function isWorkspaceIntentMeta(resource: RbacResource, _meta: IntentMeta): _meta is WorkspaceIntentMeta {
  return resource === "WORKSPACE";
}
