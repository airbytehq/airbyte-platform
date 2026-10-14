import React from "react";
import { useIntl } from "react-intl";

import { Icon, IconColor, IconType } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import {
  useAgentsSupportedDestinationDefinitionIds,
  useAgentsSupportedSourceDefinitionIds,
  useConnectionSemanticSearchEnablement,
  useOrganizationSemanticSearchEnablement,
} from "core/api";
import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

import styles from "./SemanticSearchCell.module.scss";

type SemanticSearchStatus = "configured" | "notConfigured" | "notApplicable" | "loading" | "unavailable";

/** The organization semantic search setting as the Connections table read it once for all rows. */
export type OrganizationSemanticSearchState = "loading" | "unavailable" | "on" | "off";

/** Reads the organization semantic search setting. The table calls it once and passes the result to every cell. */
export const useOrganizationSemanticSearchState = ({
  enabled,
}: {
  enabled: boolean;
}): OrganizationSemanticSearchState => {
  const { data, isInitialLoading, isError } = useOrganizationSemanticSearchEnablement({ enabled });

  return isInitialLoading ? "loading" : isError || !data ? "unavailable" : data.semanticSearchEnabled ? "on" : "off";
};

const STATUS_ICONS: Record<Exclude<SemanticSearchStatus, "notApplicable">, { type: IconType; color: IconColor }> = {
  configured: { type: "statusSuccess", color: "success" },
  notConfigured: { type: "statusError", color: "disabled" },
  loading: { type: "loading", color: "disabled" },
  unavailable: { type: "statusWarning", color: "disabled" },
};

const SemanticSearchStatusIcon: React.FC<{ status: SemanticSearchStatus }> = ({ status }) => {
  const { formatMessage } = useIntl();
  const title = formatMessage(
    { id: `tables.featureStatus.${status}` },
    { feature: formatMessage({ id: "tables.semanticSearch" }) }
  );

  return (
    <span className={styles.status} role="img" aria-label={title} title={title}>
      {status === "notApplicable" ? (
        <Text as="span" size="sm" color="grey">
          –
        </Text>
      ) : (
        <Icon {...STATUS_ICONS[status]} size="sm" />
      )}
    </span>
  );
};

// Mounted only for rows that need the per-connection value, so other rows send no enablement read.
const ConnectionSemanticSearchEnablementStatus: React.FC<{ connectionId: string }> = ({ connectionId }) => {
  const { data, isLoading, isError } = useConnectionSemanticSearchEnablement(connectionId);

  // A failed read shows "unavailable", not a cross: the saved value is unknown, so the cell must not claim it is off.
  const status: SemanticSearchStatus = isLoading
    ? "loading"
    : isError || !data
    ? "unavailable"
    : data.enable_indexing
    ? "configured"
    : "notConfigured";

  return <SemanticSearchStatusIcon status={status} />;
};

/**
 * Shows the semantic search state of one connection in the Connections list:
 * - a dash when the source or the destination is not an agent connector;
 * - a grey cross when semantic search is off for the organization or for the connection;
 * - a teal check when semantic search is on for the connection.
 * Permission does not change the cell. While a value loads, or when it cannot be loaded, the cell is neutral.
 *
 * The table renders this cell only when the Cloud, flag and Context Layer checks pass. The table reads the
 * organization setting once and passes it in, so a row reads only its own connection value, and only when the
 * organization setting is on.
 */
export const SemanticSearchCell: React.FC<{
  connection: WebBackendConnectionListItem;
  organizationSemanticSearch: OrganizationSemanticSearchState;
}> = ({ connection, organizationSemanticSearch }) => {
  const supportedSourceDefinitionIds = useAgentsSupportedSourceDefinitionIds();
  const supportedDestinationDefinitionIds = useAgentsSupportedDestinationDefinitionIds();
  const isAgentConnection =
    supportedSourceDefinitionIds.has(connection.source.sourceDefinitionId) &&
    supportedDestinationDefinitionIds.has(connection.destination.destinationDefinitionId);

  if (!isAgentConnection) {
    return <SemanticSearchStatusIcon status="notApplicable" />;
  }
  if (organizationSemanticSearch === "loading") {
    return <SemanticSearchStatusIcon status="loading" />;
  }
  if (organizationSemanticSearch === "unavailable") {
    return <SemanticSearchStatusIcon status="unavailable" />;
  }
  if (organizationSemanticSearch === "off") {
    return <SemanticSearchStatusIcon status="notConfigured" />;
  }
  return <ConnectionSemanticSearchEnablementStatus connectionId={connection.connectionId} />;
};
