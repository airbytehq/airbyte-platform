import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import {
  ActorStatus,
  ConnectionStatus,
  DestinationRead,
  DestinationSnippetRead,
  JobStatus,
  SourceRead,
  SourceSnippetRead,
  WebBackendConnectionListItem,
} from "core/api/types/AirbyteClient";

import { EntityTableDataItem, ConnectionTableDataItem, Status as ConnectionSyncStatus } from "./types";

const getConnectorTypeName = (connectorSpec: DestinationSnippetRead | SourceSnippetRead) => {
  return "sourceName" in connectorSpec ? connectorSpec.sourceName : connectorSpec.destinationName;
};

const getConnectorTypeId = (connectorSpec: DestinationSnippetRead | SourceSnippetRead) => {
  return "sourceId" in connectorSpec ? connectorSpec.sourceId : connectorSpec.destinationId;
};

// TODO: types in next methods look a bit ugly
export function getEntityTableData<
  S extends "source" | "destination",
  SoD extends S extends "source" ? SourceRead : DestinationRead,
>(entities: SoD[], connections: WebBackendConnectionListItem[], type: S): EntityTableDataItem[] {
  const connectType = type === "source" ? "destination" : "source";

  const mappedEntities = entities.map((entityItem) => {
    const entitySoDId = entityItem[`${type}Id` as keyof SoD] as unknown as string;
    const entitySoDName = entityItem[`${type}Name` as keyof SoD] as unknown as string;
    const entityConnections = connections.filter(
      (connectionItem) => getConnectorTypeId(connectionItem[type]) === entitySoDId
    );

    if (!entityConnections.length) {
      return {
        entityId: entitySoDId,
        entityName: entityItem.name,
        enabled: true,
        connectorName: entitySoDName,
        connectorIcon: entityItem.icon,
        lastSync: null,
        connectEntities: [],
        isActive: entityItem.status === ActorStatus.active,
        breakingChanges: entityItem.breakingChanges,
        isVersionOverrideApplied: entityItem.isVersionOverrideApplied ?? false,
        supportState: entityItem.supportState,
      };
    }

    const connectEntities = entityConnections.map((connection) => ({
      name: connection[connectType]?.name || "",
      connector: getConnectorTypeName(connection[connectType]),
      status: connection.status,
      lastSyncStatus: getConnectionSyncStatus(connection.status, connection.latestSyncJobStatus),
    }));

    const sortBySync = entityConnections.sort((item1, item2) =>
      item1.latestSyncJobCreatedAt && item2.latestSyncJobCreatedAt
        ? item2.latestSyncJobCreatedAt - item1.latestSyncJobCreatedAt
        : 0
    );

    return {
      entityId: entitySoDId,
      entityName: entityItem.name,
      enabled: true,
      connectorName: entitySoDName,
      lastSync: sortBySync?.[0].latestSyncJobCreatedAt,
      connectEntities,
      connectorIcon: entityItem.icon,
      isActive: entityItem.status === ActorStatus.active,
      breakingChanges: entityItem.breakingChanges,
      isVersionOverrideApplied: entityItem.isVersionOverrideApplied ?? false,
      supportState: entityItem.supportState,
    };
  });

  return mappedEntities;
}

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

const generateStatusFilterOption = (value: string | null, id: string) => ({
  label: (
    <Text color="grey" bold as="span">
      <FormattedMessage id={id} />
    </Text>
  ),
  value,
});

export const statusFilterOptions = [
  generateStatusFilterOption(null, "tables.connectors.filters.status.all"),
  generateStatusFilterOption("inactive", "tables.connectors.filters.status.inactive"),
  generateStatusFilterOption("active", "tables.connectors.filters.status.active"),
];

/**
 * Filter entity table data by entityName(name defined by user) and connectorName
 * @param searchFilter
 * @param data
 */
export const filterBySearchEntityTableData = (
  searchFilter: string,
  status: string | null,
  data: EntityTableDataItem[]
) =>
  data.filter(
    ({ entityName, connectorName, isActive }) =>
      [entityName, connectorName]
        .map((value) => value.toLowerCase())
        .some((value) => value.includes(searchFilter.toLowerCase())) &&
      (status === null || (isActive && status === "active") || (!isActive && status === "inactive"))
  );
