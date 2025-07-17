import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import {
  ActorStatus,
  ConnectionStatus,
  DestinationSnippetRead,
  JobStatus,
  SourceSnippetRead,
  WebBackendConnectionListItem,
} from "core/api/types/AirbyteClient";

import { ConnectionTableDataItem, Status as ConnectionSyncStatus } from "./types";

const getConnectorTypeName = (connectorSpec: DestinationSnippetRead | SourceSnippetRead) => {
  return "sourceName" in connectorSpec ? connectorSpec.sourceName : connectorSpec.destinationName;
};

export const getConnectionTableData = (
  connections: WebBackendConnectionListItem[],
  type: "source" | "destination" | "connection"
): ConnectionTableDataItem[] => {
  const connectType = type === "source" ? "destination" : "source";

  return connections.map((connection) => ({
    connectionId: connection.connectionId,
    name: connection.name,
    entityName: type === "connection" ? connection.source?.name : connection[connectType]?.name || "",
    connectorName: type === "connection" ? connection.destination?.name : getConnectorTypeName(connection[connectType]),
    lastSync: connection.latestSyncJobCreatedAt,
    enabled: connection.status === ConnectionStatus.active,
    schemaChange: connection.schemaChange,
    source: connection.sourceActorDefinitionVersion,
    destination: connection.destinationActorDefinitionVersion,
    scheduleData: connection.scheduleData,
    scheduleType: connection.scheduleType,
    status: connection.status,
    isSyncing: connection.isSyncing,
    lastSyncStatus: getConnectionSyncStatus(connection.status, connection.latestSyncJobStatus),
    connectorIcon: type === "destination" ? connection.source.icon : connection.destination.icon,
    entityIcon: type === "destination" ? connection.destination.icon : connection.source.icon,
    connection,
    tags: connection.tags,
  }));
};

export const getConnectionSyncStatus = (
  status: ConnectionStatus,
  lastSyncJobStatus: JobStatus | undefined
): ConnectionSyncStatus => {
  if (status === ConnectionStatus.inactive) {
    return ConnectionSyncStatus.INACTIVE;
  }

  switch (lastSyncJobStatus) {
    case JobStatus.succeeded:
      return ConnectionSyncStatus.ACTIVE;

    case JobStatus.failed:
      return ConnectionSyncStatus.FAILED;

    case JobStatus.cancelled:
      return ConnectionSyncStatus.CANCELLED;

    case JobStatus.pending:
    case JobStatus.running:
    case JobStatus.incomplete:
      return ConnectionSyncStatus.PENDING;

    default:
      return ConnectionSyncStatus.EMPTY;
  }
};

const generateStatusFilterOption = (value: ActorStatus | null, id: string) => ({
  label: (
    <Text color="grey" bold as="span">
      <FormattedMessage id={id} />
    </Text>
  ),
  value,
});

export const statusFilterOptions = [
  generateStatusFilterOption(null, "tables.connectors.filters.status.all"),
  generateStatusFilterOption(ActorStatus.inactive, "tables.connectors.filters.status.inactive"),
  generateStatusFilterOption(ActorStatus.active, "tables.connectors.filters.status.active"),
];
