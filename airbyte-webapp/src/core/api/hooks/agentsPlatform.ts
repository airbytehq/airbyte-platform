import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useWebappConfig } from "core/config/webappConfig";

import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

// Temporary hardcoded list pending a Sonar endpoint for supported source definitions.
const DEFAULT_SUPPORTED_SOURCE_DEFINITIONS = ["GitHub", "Stripe", "Salesforce", "Google Sheets", "Postgres", "Shopify"];
const AGENTS_SUPPORTED_SOURCE_DEFINITIONS = new Set(DEFAULT_SUPPORTED_SOURCE_DEFINITIONS);

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
  externalWorkspaceConnectors: (organizationId: string, workspaceId: string) =>
    [SCOPE_ORGANIZATION, "agentsPlatform", "externalWorkspaceConnectors", organizationId, workspaceId] as const,
};

export const useAgentsProvisioningStatus = ({ enabled = true }: { enabled?: boolean } = {}) => {
  const { sonarApiUrl: baseUrl } = useWebappConfig();
  const organizationId = useCurrentOrganizationId();
  const { getAccessToken } = useRequestOptions();
  const queryKey = agentsPlatformKeys.provisioningStatus(organizationId);

  return useQuery<AgentsProvisioningStatus | null>(
    queryKey,
    async () => {
      try {
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
      } catch {
        return null;
      }
    },
    {
      enabled: !!baseUrl && enabled,
      retry: false,
      staleTime: 5 * 60 * 1000,
    }
  ).data;
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

export const useAgentsSupportedSourceDefinitions = (): Set<string> => {
  return AGENTS_SUPPORTED_SOURCE_DEFINITIONS;
};

interface ExternalWorkspaceConnector {
  id: string;
  name: string;
  supported: boolean;
  enabled: boolean;
}

interface ExternalWorkspaceConnectorResponse {
  actor_id?: string;
  destination_id?: string;
  name: string;
  source_type?: string;
  destination_type?: string;
  definition_id: string;
  workspace_id: string;
  created_at: string;
  supported: boolean;
  enabled: boolean;
}

interface ExternalActorEnabledResponse {
  organization_id: string;
  actor_id: string;
  actor_kind: "source" | "destination";
  enabled: boolean;
}

export const useExternalWorkspaceConnectors = (workspaceId: string) => {
  const { sonarApiUrl: baseUrl } = useWebappConfig();
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryKey = agentsPlatformKeys.externalWorkspaceConnectors(organizationId, workspaceId);

  const fetchConnectors = async (actorKind: "source" | "destination"): Promise<ExternalWorkspaceConnector[]> => {
    const accessToken = await requestOptions.getAccessToken();
    const response = await fetch(
      `${baseUrl}/api/v1/organizations/external/${organizationId}/workspaces/${workspaceId}/${actorKind}s`,
      {
        headers: {
          "X-Organization-Id": organizationId,
          ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
        },
      }
    );

    if (!response.ok) {
      throw new Error(`External ${actorKind} connector request failed: ${response.status}`);
    }

    const { data } = (await response.json()) as { data: ExternalWorkspaceConnectorResponse[] };
    return data.flatMap((connector) => {
      const id = actorKind === "source" ? connector.actor_id : connector.destination_id;
      return id ? [{ id, name: connector.name, supported: connector.supported, enabled: connector.enabled }] : [];
    });
  };

  const sourcesQuery = useQuery([...queryKey, "sources"], () => fetchConnectors("source"), {
    enabled: Boolean(workspaceId && baseUrl),
    retry: false,
    staleTime: 5 * 60 * 1000,
  });

  const destinationsQuery = useQuery([...queryKey, "destinations"], () => fetchConnectors("destination"), {
    enabled: Boolean(workspaceId && baseUrl),
    retry: false,
    staleTime: 5 * 60 * 1000,
  });

  return {
    sources: sourcesQuery.data ?? [],
    destinations: destinationsQuery.data ?? [],
    isLoading: sourcesQuery.isLoading || destinationsQuery.isLoading,
    sourcesError: sourcesQuery.isError,
    destinationsError: destinationsQuery.isError,
  };
};

export const useSetExternalActorEnabled = () => {
  const { sonarApiUrl: baseUrl } = useWebappConfig();
  const organizationId = useCurrentOrganizationId();
  const { getAccessToken } = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    async ({
      actorId,
      actorKind,
      enabled,
    }: {
      actorId: string;
      actorKind: "source" | "destination";
      enabled: boolean;
    }) => {
      if (!baseUrl) {
        throw new Error("Sonar API URL is not configured");
      }

      const accessToken = await getAccessToken();
      const response = await fetch(
        `${baseUrl}/api/v1/organizations/external/${organizationId}/actors/${actorId}?actor_kind=${actorKind}`,
        {
          method: enabled ? "POST" : "DELETE",
          headers: {
            "X-Organization-Id": organizationId,
            ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
          },
        }
      );

      if (!response.ok) {
        throw new Error(`External actor request failed: ${response.status}`);
      }

      return (await response.json()) as ExternalActorEnabledResponse;
    },
    {
      onSuccess: async () => {
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
