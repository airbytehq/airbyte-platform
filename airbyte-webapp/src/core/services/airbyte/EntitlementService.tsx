import React, { useCallback, useEffect } from "react";

import { getEntitlements, useRequestOptions } from "core/api";
import { FeatureItem, FeatureSet, useFeatureService } from "core/services/features";

import { useCurrentOrganizationId } from "../../../area/organization/utils";

const idToFeatureItem: Record<string, FeatureItem> = {
  "feature-fe-allow-all-rbac-roles": FeatureItem.AllowAllRBACRoles,
  "feature-connection-hashing-ui-v0": FeatureItem.FieldHashing,
  "feature-fe-cloud-for-teams-branding": FeatureItem.CloudForTeamsBranding,
  "feature-fe-display-organization-users": FeatureItem.DisplayOrganizationUsers,
  "feature-fe-indicate-guest-users": FeatureItem.IndicateGuestUsers,
  "feature-connection-mappings-ui": FeatureItem.MappingsUI,
};

const EntitlementBootstrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { setEntitlementOverwrites } = useFeatureService();
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();

  const refreshEntitlements = useCallback(async () => {
    if (!organizationId) {
      return;
    }
    try {
      const { entitlements } = await getEntitlements({ organization_id: organizationId }, requestOptions);
      const featureSet: FeatureSet = {};
      for (const entitlement of entitlements) {
        const featureItem = idToFeatureItem[entitlement.feature_id];
        if (featureItem) {
          featureSet[featureItem] = entitlement.is_entitled;
        }
      }
      setEntitlementOverwrites(featureSet);
    } catch (e) {
      console.error("Failed to fetch entitlements from Airbyte server", e);
    }
  }, [organizationId, setEntitlementOverwrites, requestOptions]);

  useEffect(() => {
    refreshEntitlements();
  }, [refreshEntitlements]);

  return <>{children}</>;
};

export const AirbyteEntitlementProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  return <EntitlementBootstrapper>{children}</EntitlementBootstrapper>;
};
