import { useQuery } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import { listAuditLogs } from "../generated/AirbyteClient";
import { AuditLogListRequestBody } from "../generated/AirbyteClient.schemas";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

export type AuditLogListFilters = Omit<AuditLogListRequestBody, "organizationId" | "pageSize" | "pageToken">;

export const auditLogKeys = {
  all: [SCOPE_ORGANIZATION, "auditLogs"] as const,
  list: (organizationId: string, filters: AuditLogListFilters, pageToken?: string) =>
    [...auditLogKeys.all, "list", organizationId, filters, pageToken ?? null] as const,
};

export const AUDIT_LOGS_PAGE_SIZE = 50;

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
    { enabled: Boolean(organizationId), keepPreviousData: true }
  );
};
