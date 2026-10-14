import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  useAgentsSupportedDestinationDefinitionIds,
  useAgentsSupportedSourceDefinitionIds,
  useOrganizationSemanticSearchEnablement,
} from "core/api";
import { useIntent } from "core/utils/rbac";

import { useConnectionSemanticSearchAvailability } from "./useConnectionSemanticSearchAvailability";

export type ConnectionSemanticSearchAccess = "loading" | "hidden" | "orgOff" | "noPermission" | "editable";

interface ConnectionSemanticSearchAccessParams {
  sourceDefinitionId?: string;
  destinationDefinitionId?: string;
}

/**
 * Decides how a connection-level semantic search control renders.
 * - "hidden": not Cloud, the semantic search UI flag is off, either connector is not an agent connector, the Context
 *   Layer is not available, or the Context Layer status or organization semantic search setting failed to load (fail
 *   closed).
 * - "loading": the Context Layer status or the organization setting is still loading. Callers must wait for another
 *   value before they pick a default.
 * - "orgOff": semantic search is off for the organization.
 * - "noPermission": the user cannot edit semantic search for connections in the current workspace.
 * - "editable": otherwise.
 *
 * The Cloud, flag, and connector checks run first, so those connections are never "loading".
 *
 * The Cloud, flag, and Context Layer checks come from useConnectionSemanticSearchAvailability, which the Connections
 * list also uses. Only the organization semantic search setting is gated: it is read only after those checks and the
 * connector check pass, because OSS does not implement that endpoint. The reads are shared queries, so many rows can
 * call the hook at little cost.
 *
 * The organization read (useOrganization, for the ADP check) is a suspense query: it suspends while it loads and throws
 * to the error boundary when it fails, the same as the sidebar's read of the same query.
 */
export const useConnectionSemanticSearchAccess = ({
  sourceDefinitionId,
  destinationDefinitionId,
}: ConnectionSemanticSearchAccessParams): ConnectionSemanticSearchAccess => {
  const availability = useConnectionSemanticSearchAvailability();
  const supportedSourceDefinitionIds = useAgentsSupportedSourceDefinitionIds();
  const supportedDestinationDefinitionIds = useAgentsSupportedDestinationDefinitionIds();
  const isAgentConnection =
    !!sourceDefinitionId &&
    !!destinationDefinitionId &&
    supportedSourceDefinitionIds.has(sourceDefinitionId) &&
    supportedDestinationDefinitionIds.has(destinationDefinitionId);

  const organizationSettingQuery = useOrganizationSemanticSearchEnablement({
    enabled: isAgentConnection && availability === "available",
  });

  const workspaceId = useCurrentWorkspaceId();
  const canEdit = useIntent("EditConnectionSemanticSearch", { workspaceId });

  if (!isAgentConnection || availability === "unavailable") {
    return "hidden";
  }
  if (availability === "loading" || organizationSettingQuery.isInitialLoading) {
    return "loading";
  }
  const organizationSetting = organizationSettingQuery.isError ? undefined : organizationSettingQuery.data;
  if (!organizationSetting) {
    return "hidden";
  }
  if (!organizationSetting.semanticSearchEnabled) {
    return "orgOff";
  }
  return canEdit ? "editable" : "noPermission";
};
