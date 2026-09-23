import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { ConnectorIds } from "area/connector/utils/constants";
import { useCurrentOrganizationId } from "area/organization/utils";
import { useWebappConfig } from "core/config/webappConfig";

import { AGENTS_SUPPORTED_SOURCE_DEFINITION_IDS } from "./agentsSupportedSourceDefinitionIds.generated";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

const AGENTS_SUPPORTED_SOURCE_DEFINITION_IDS_SET = new Set(AGENTS_SUPPORTED_SOURCE_DEFINITION_IDS);

// Destination definitions supported by Sonar SQL passthrough (airbytehq/sonar#6449).
const AGENTS_SUPPORTED_DESTINATION_DEFINITION_IDS = new Set([
  ConnectorIds.Destinations.Snowflake, // Snowflake
  ConnectorIds.Destinations.BigQuery, // BigQuery
]);

export interface AgentsProvisioningStatus {
  is_enrolled: boolean;
  is_instance_admin: boolean;
  provisioning_state: string | null;
  organization_id: string | null;
  organization_kind: string | null;
  external_cloud_eligible: boolean;
  eligible_external_organization_id: string | null;
}

export const agentsPlatformKeys = {
  provisioningStatus: (organizationId: string) =>
    [SCOPE_ORGANIZATION, "agentsPlatform", "provisioningStatus", organizationId] as const,
};

export const useAgentsProvisioningStatusQuery = ({ enabled = true }: { enabled?: boolean } = {}) => {
  const { sonarApiUrl: baseUrl } = useWebappConfig();
  const organizationId = useCurrentOrganizationId();
  const { getAccessToken } = useRequestOptions();
  const queryKey = agentsPlatformKeys.provisioningStatus(organizationId);

  return useQuery<AgentsProvisioningStatus | null>(
    queryKey,
    async () => {
      const accessToken = await getAccessToken();
      const response = await fetch(`${baseUrl}/api/v1/internal/account/provisioning-check`, {
        headers: {
          "X-Organization-Id": organizationId,
          ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
        },
      });

      if (!response.ok) {
        throw new Error(`Agents provisioning status request failed: ${response.status}`);
      }

      const status = (await response.json()) as AgentsProvisioningStatus;
      if (
        (status.is_enrolled && status.organization_id === organizationId) ||
        (status.external_cloud_eligible && status.eligible_external_organization_id === organizationId)
      ) {
        return status;
      }

      return null;
    },
    {
      enabled: !!baseUrl && enabled,
      retry: false,
      staleTime: 5 * 60 * 1000,
    }
  );
};

export const useAgentsProvisioningStatus = ({ enabled = true }: { enabled?: boolean } = {}) => {
  return useAgentsProvisioningStatusQuery({ enabled }).data;
};

export const useEnrollOrganizationInAgents = () => {
  const { sonarApiUrl: baseUrl } = useWebappConfig();
  const organizationId = useCurrentOrganizationId();
  const { getAccessToken } = useRequestOptions();
  const queryClient = useQueryClient();
  const queryKey = agentsPlatformKeys.provisioningStatus(organizationId);

  return useMutation(
    async ({ workspaceIds, addAllSupportedActors }: { workspaceIds: string[]; addAllSupportedActors: boolean }) => {
      if (!baseUrl) {
        throw new Error("Agents API URL is not configured");
      }

      const accessToken = await getAccessToken();
      const response = await fetch(`${baseUrl}/api/v1/organizations/external`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Organization-Id": organizationId,
          ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
        },
        body: JSON.stringify({
          organization_id: organizationId,
          workspace_ids: workspaceIds,
          add_all_supported_actors: addAllSupportedActors,
        }),
      });

      if (!response.ok) {
        throw new Error(`Agents enrollment request failed: ${response.status}`);
      }
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries(queryKey);
      },
    }
  );
};

export const useUnenrollOrganizationFromAgents = () => {
  const { sonarApiUrl: baseUrl } = useWebappConfig();
  const organizationId = useCurrentOrganizationId();
  const { getAccessToken } = useRequestOptions();
  const queryClient = useQueryClient();
  const queryKey = agentsPlatformKeys.provisioningStatus(organizationId);

  return useMutation(
    async () => {
      if (!baseUrl) {
        throw new Error("Agents API URL is not configured");
      }

      const accessToken = await getAccessToken();
      const response = await fetch(`${baseUrl}/api/v1/organizations/external/${organizationId}`, {
        method: "DELETE",
        headers: {
          "X-Organization-Id": organizationId,
          ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
        },
      });

      if (!response.ok) {
        throw new Error(`Agents disable request failed: ${response.status}`);
      }
    },
    {
      onSuccess: async () => {
        await queryClient.invalidateQueries(queryKey);
        await queryClient.invalidateQueries([
          SCOPE_ORGANIZATION,
          "agentsPlatform",
          "externalWorkspaceConnectors",
          organizationId,
        ]);
      },
    }
  );
};

export const useAgentsSupportedSourceDefinitionIds = (): Set<string> => {
  return AGENTS_SUPPORTED_SOURCE_DEFINITION_IDS_SET;
};

export const useAgentsSupportedDestinationDefinitionIds = (): Set<string> => {
  return AGENTS_SUPPORTED_DESTINATION_DEFINITION_IDS;
};
