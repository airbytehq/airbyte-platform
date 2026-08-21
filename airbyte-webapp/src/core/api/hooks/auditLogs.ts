import { useQuery } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import { listAuditLogs } from "../generated/AirbyteClient";
import { AuditLogListRequestBody, AuditLogReadList } from "../generated/AirbyteClient.schemas";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

export type AuditLogListFilters = Omit<AuditLogListRequestBody, "organizationId" | "pageSize" | "pageToken">;

export const auditLogKeys = {
  all: [SCOPE_ORGANIZATION, "auditLogs"] as const,
  list: (organizationId: string, filters: AuditLogListFilters, pageToken?: string) =>
    [...auditLogKeys.all, "list", organizationId, filters, pageToken ?? null] as const,
};

export const AUDIT_LOGS_PAGE_SIZE = 50;

const WEB_BACKEND_PREFIX = "webBackend";

/**
 * Drops the `webBackend` prefix from an operation name, e.g. `webBackendUpdateConnection` becomes
 * `updateConnection`.
 *
 * The webBackend endpoints are an implementation detail of how this app talks to the server: an
 * update made in the UI is recorded as `webBackendUpdateConnection` while the same update made
 * through the public API is recorded as `updateConnection`. To someone reading the audit trail
 * those are one action, so the prefix is noise.
 *
 * Safe to apply to the filter value too, because the API matches `operation` as a case-insensitive
 * substring — `updateConnection` still selects the stored `webBackendUpdateConnection` entries.
 */
const normalizeOperation = (operation: string): string => {
  if (!operation.startsWith(WEB_BACKEND_PREFIX)) {
    return operation;
  }
  const remainder = operation.slice(WEB_BACKEND_PREFIX.length);
  return remainder ? remainder.charAt(0).toLowerCase() + remainder.slice(1) : operation;
};

const normalizeOperations = (list: AuditLogReadList): AuditLogReadList => ({
  ...list,
  auditLogs: list.auditLogs.map((entry) => ({ ...entry, operation: normalizeOperation(entry.operation) })),
});

/**
 * Deliberately a plain `useQuery` (same rationale as `useListGroups`): the endpoint is
 * @Secured(ORGANIZATION_ADMIN), so a 403 is a reachable state for a real user and should
 * render inline on the page rather than escape to the app-level error boundary via the
 * settings shell's Suspense boundary.
 */
export const useListAuditLogs = (filters: AuditLogListFilters, pageToken?: string) => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();

  return useQuery(
    auditLogKeys.list(organizationId, filters, pageToken),
    () => listAuditLogs({ organizationId, ...filters, pageSize: AUDIT_LOGS_PAGE_SIZE, pageToken }, requestOptions),
    { enabled: Boolean(organizationId), keepPreviousData: true, select: normalizeOperations }
  );
};
