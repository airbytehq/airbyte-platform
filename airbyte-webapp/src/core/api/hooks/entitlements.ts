import { useCurrentOrganizationId } from "area/organization/utils";
import { useFeatureService } from "core/services/features/FeatureService";
import { FeatureItem, FeatureSet } from "core/services/features/types";
import { trackError } from "core/utils/datadog";

import { getEntitlements } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { GetEntitlementsByOrganizationIdResponse } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export interface CheckEntitlementResult {
  isEntitled: boolean | null;
  error: Error | null;
  isLoading: boolean;
}

const entitlementsKeys = {
  all: [SCOPE_ORGANIZATION, "entitlements"] as const,
  byOrganization: (organizationId: string) => [...entitlementsKeys.all, organizationId] as const,
  checkEntitlement: (organizationId: string, featureId: string) =>
    [...entitlementsKeys.byOrganization(organizationId), "check", featureId] as const,
};

const entitlementIdToFeatureItem: Record<string, FeatureItem> = {
  "feature-audit-logging": FeatureItem.AllowAuditLogs,
  "feature-rbac-roles": FeatureItem.AllowAllRBACRoles,
  "feature-fe-display-organization-users": FeatureItem.DisplayOrganizationUsers,
  "feature-fe-indicate-guest-users": FeatureItem.IndicateGuestUsers,
  "feature-mappers": FeatureItem.MappingsUI,
  "feature-sso": FeatureItem.AllowUpdateSSOConfig,
  "feature-maximum-workspaces": FeatureItem.CreateMultipleWorkspaces,
  "feature-ai-copilot": FeatureItem.AICopilot,
  "feature-faster-sync-frequency": FeatureItem.AllowSyncFrequencyUnderOneHour,
  "feature-15-minute-sync-frequency": FeatureItem.AllowSyncFrequencyUnderOneHour,
  "feature-committed-data-workers": FeatureItem.AllowDataWorkerCapacity,
  "feature-data-worker-capacity": FeatureItem.AllowDataWorkerCapacity, // legacy fallback
  "feature-on-demand-capacity-enabled": FeatureItem.OnDemandCapacity,
  "feature-self-managed-regions": FeatureItem.SelfManagedRegions,
  "feature-privatelink": FeatureItem.PrivateLinks,
};

/**
 * Fetches the entitlements for an organization and applies them to the FeatureService.
 * Shared by all entitlement hooks so the side effect runs no matter which observer wins the
 * query deduplication.
 */
async function fetchEntitlements(
  organizationId: string,
  requestOptions: ReturnType<typeof useRequestOptions>,
  setEntitlementOverwrites: ReturnType<typeof useFeatureService>["setEntitlementOverwrites"]
): Promise<GetEntitlementsByOrganizationIdResponse> {
  try {
    const { entitlements } = await getEntitlements({ organization_id: organizationId }, requestOptions);

    // Apply entitlements immediately during fetch
    const featureSet: FeatureSet = {};
    for (const entitlement of entitlements) {
      const featureItem = entitlementIdToFeatureItem[entitlement.feature_id];
      if (featureItem) {
        featureSet[featureItem] = entitlement.is_entitled;
      }
    }
    setEntitlementOverwrites(featureSet);

    return { entitlements };
  } catch (error) {
    trackError(error, { context: "entitlements_fetch", orgId: organizationId });
    throw error;
  }
}

/**
 * Hook to fetch and set entitlements for an organization
 * Fetches entitlements from the API and automatically applies them to FeatureService
 * Uses Suspense for loading states and error boundaries
 */
export const useSetEntitlements = (): void => {
  const requestOptions = useRequestOptions();
  const currentOrganizationId = useCurrentOrganizationId();
  const { setEntitlementOverwrites } = useFeatureService();

  useSuspenseQuery<GetEntitlementsByOrganizationIdResponse>(
    entitlementsKeys.byOrganization(currentOrganizationId ?? ""),
    () => fetchEntitlements(currentOrganizationId ?? "", requestOptions, setEntitlementOverwrites),
    { enabled: !!currentOrganizationId }
  );
};

/**
 * Returns the maximum number of workspaces the current organization may have, according to the
 * feature-maximum-workspaces entitlement. Returns null when there is no finite limit (no
 * entitlement, unlimited, or no organization).
 */
export const useMaximumWorkspaces = (): number | null => {
  const requestOptions = useRequestOptions();
  const currentOrganizationId = useCurrentOrganizationId();
  const { setEntitlementOverwrites } = useFeatureService();

  const entitlementsResponse = useSuspenseQuery<GetEntitlementsByOrganizationIdResponse>(
    entitlementsKeys.byOrganization(currentOrganizationId ?? ""),
    () => fetchEntitlements(currentOrganizationId ?? "", requestOptions, setEntitlementOverwrites),
    { enabled: !!currentOrganizationId }
  );

  const entitlement = entitlementsResponse?.entitlements.find((e) => e.feature_id === "feature-maximum-workspaces");
  if (!entitlement?.is_entitled || entitlement.is_unlimited || entitlement.value == null) {
    return null;
  }
  return entitlement.value;
};
