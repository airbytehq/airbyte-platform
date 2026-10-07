import { useQuery } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useIsCloudApp } from "core/utils/app";

import { getFusionUsage } from "../../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../../scopes";
import { useRequestOptions } from "../../useRequestOptions";
import { useAgentsProvisioningStatus } from "../agentsPlatform";

export const fusionUsageKeys = {
  all: [SCOPE_ORGANIZATION, "fusionUsage"] as const,
  detail: (organizationId: string) => [...fusionUsageKeys.all, organizationId] as const,
};

/** Conditional read with inline states so usage failures never replace the page or app shell. */
export const useFusionUsage = () => {
  const organizationId = useCurrentOrganizationId();
  const isCloudApp = useIsCloudApp();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp });
  const isEligible = Boolean(
    isCloudApp &&
      organizationId &&
      status?.is_enrolled &&
      status.organization_kind === "external_cloud" &&
      status.organization_id === organizationId
  );
  const requestOptions = useRequestOptions();

  const query = useQuery(
    fusionUsageKeys.detail(organizationId),
    async () => {
      const usage = await getFusionUsage(organizationId, requestOptions);
      if (usage.organizationId !== organizationId) {
        throw new Error("Fusion usage response does not match the current organization");
      }
      return usage;
    },
    {
      enabled: isEligible,
      retry: false,
      staleTime: 30_000,
      refetchInterval: 60_000,
    }
  );

  return { ...query, isEligible };
};
