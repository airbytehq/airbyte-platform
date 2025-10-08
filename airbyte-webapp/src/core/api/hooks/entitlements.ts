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
  "feature-rbac-roles": FeatureItem.AllowAllRBACRoles,
  "feature-fe-display-organization-users": FeatureItem.DisplayOrganizationUsers,
  "feature-fe-indicate-guest-users": FeatureItem.IndicateGuestUsers,
  "feature-mappers": FeatureItem.MappingsUI,
  "feature-sso": FeatureItem.AllowUpdateSSOConfig,
  "feature-multiple-workspaces": FeatureItem.CreateMultipleWorkspaces,
  "feature-ai-copilot": FeatureItem.AICopilot,
  "feature-faster-sync-frequency": FeatureItem.AllowSyncFrequencyUnderOneHour,
};

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
    async () => {
      try {
        const { entitlements } = await getEntitlements({ organization_id: currentOrganizationId }, requestOptions);

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
        trackError(error, { context: "entitlements_fetch", orgId: currentOrganizationId });
        throw error;
      }
    },
    { enabled: !!currentOrganizationId }
  );
};
