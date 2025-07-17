import { ColumnSort, createColumnHelper } from "@tanstack/react-table";
import React, { useContext, useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { AllConnectionsStatusCell } from "components/EntityTable/components/AllConnectionsStatusCell";
import { ConnectorName } from "components/EntityTable/components/ConnectorName";
import { EntityNameCell } from "components/EntityTable/components/EntityNameCell";
import { LastSyncCell } from "components/EntityTable/components/LastSyncCell";
import { NumberOfConnectionsCell } from "components/EntityTable/components/NumberOfConnectionsCell";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { ScrollParentContext } from "components/ui/ScrollParent";
import { Table } from "components/ui/Table";
import { Tooltip } from "components/ui/Tooltip";

import {
  ActorDefinitionVersionBreakingChanges,
  ActorListSortKey,
  ActorStatus,
  DestinationReadList,
  SourceReadList,
  SupportState,
} from "core/api/types/AirbyteClient";
import { getHumanReadableUpgradeDeadline, shouldDisplayBreakingChangeBanner } from "core/domain/connector";
import { FeatureItem, useFeature } from "core/services/features";
import { getBreakingChangeErrorMessage } from "pages/connections/StreamStatusPage/ConnectionStatusMessages";

import styles from "./ActorTable.module.scss";

interface ActorTableProps {
  actorReadList: SourceReadList | DestinationReadList;
  hasNextPage: boolean;
  fetchNextPage: () => void;
  sortKey?: ActorListSortKey;
  setSortKey?: (sortState: ActorListSortKey) => void;
}

function isSourceReadList(list: SourceReadList | DestinationReadList): list is SourceReadList {
  return "sources" in list;
}

interface ActorTableDataItem {
  actorType: "source" | "destination";
  id: string;
  actorName: string;
  actorDefinitionName: string;
  enabled: boolean;
  connectorIcon?: string;
  isActive: boolean;
  breakingChanges?: ActorDefinitionVersionBreakingChanges;
  isVersionOverrideApplied: boolean;
  supportState?: SupportState;
  numConnections?: number;
  lastSync?: number;
  connectionJobStatuses?: Record<string, number>;
}

type ActorTableSortableColumns = keyof Pick<ActorTableDataItem, "actorName" | "actorDefinitionName" | "lastSync">;

function isActorTableSortableColumn(id: unknown): id is ActorTableSortableColumns {
  return id === "actorName" || id === "actorDefinitionName" || id === "lastSync";
}

function getColumnFromSortingState(sortingState: ColumnSort[]): ActorTableSortableColumns {
  return isActorTableSortableColumn(sortingState[0]?.id) ? sortingState[0].id : "actorName";
}

const sortingKeyToColumnId: Record<ActorListSortKey, ColumnSort> = {
  actorName_asc: { id: "actorName", desc: false },
  actorName_desc: { id: "actorName", desc: true },
  actorDefinitionName_asc: { id: "actorDefinitionName", desc: false },
  actorDefinitionName_desc: { id: "actorDefinitionName", desc: true },
  lastSync_asc: { id: "lastSync", desc: false },
  lastSync_desc: { id: "lastSync", desc: true },
};

export function createActorTableData(actorReadList: SourceReadList | DestinationReadList): ActorTableDataItem[] {
  if (isSourceReadList(actorReadList)) {
    return actorReadList.sources.map((source) => ({
      actorType: "source",
      id: source.sourceId,
      actorName: source.name,
      actorDefinitionName: source.sourceName,
      enabled: true,
      connectorIcon: source.icon,
      isActive: source.status === ActorStatus.active,
      breakingChanges: source.breakingChanges,
      isVersionOverrideApplied: source.isVersionOverrideApplied ?? false,
      supportState: source.supportState,
      numConnections: source.numConnections ?? 0,
      connectionJobStatuses: source.connectionJobStatuses,
      lastSync: source.lastSync,
    }));
  }
  return actorReadList.destinations.map((destination) => ({
    actorType: "destination",
    id: destination.destinationId,
    actorName: destination.name,
    actorDefinitionName: destination.destinationName,
    enabled: true,
    connectorIcon: destination.icon,
    isActive: destination.status === ActorStatus.active,
    breakingChanges: destination.breakingChanges,
    isVersionOverrideApplied: destination.isVersionOverrideApplied ?? false,
    supportState: destination.supportState,
    numConnections: destination.numConnections ?? 0,
    connectionJobStatuses: destination.connectionJobStatuses,
    lastSync: destination.lastSync,
  }));
}

const columnHelper = createColumnHelper<ActorTableDataItem>();

export const ActorTable: React.FC<ActorTableProps> = ({
  actorReadList,
  hasNextPage,
  fetchNextPage,
  sortKey,
  setSortKey,
}) => {
  const connectorBreakingChangeDeadlinesEnabled = useFeature(FeatureItem.ConnectorBreakingChangeDeadlines);

  const tableData = useMemo(() => createActorTableData(actorReadList), [actorReadList]);

  const columns = React.useMemo(
    () => [
      columnHelper.accessor("actorName", {
        header: () => <FormattedMessage id="tables.name" />,
        meta: {
          thClassName: styles.thEntityName,
          noPadding: true,
          responsive: true,
        },
        cell: (props) => (
          <Link to={props.row.original.id} variant="primary" className={styles.cellContent}>
            <EntityNameCell value={props.cell.getValue()} enabled={props.row.original.enabled} />
          </Link>
        ),
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("actorDefinitionName", {
        header: () => <FormattedMessage id="tables.connector" />,
        meta: {
          noPadding: true,
          thClassName: styles.thConnectorName,
          responsive: true,
        },
        cell: (props) => (
          <Link to={props.row.original.id} variant="primary" className={styles.cellContent}>
            <ConnectorName
              value={props.cell.getValue()}
              icon={props.row.original.connectorIcon}
              enabled={props.row.original.enabled}
            />
          </Link>
        ),
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("numConnections", {
        header: () => <FormattedMessage id="tables.connector.connections" />,
        meta: {
          noPadding: true,
          thClassName: styles.thConnectEntities,
          responsive: true,
        },
        cell: (props) => (
          <Link to={props.row.original.id} variant="primary" className={styles.cellContent}>
            <NumberOfConnectionsCell numConnections={props.cell.getValue()} enabled={props.row.original.enabled} />
          </Link>
        ),
        enableSorting: false,
      }),
      columnHelper.accessor("lastSync", {
        header: () => <FormattedMessage id="tables.lastSync" />,
        meta: {
          noPadding: true,
        },
        cell: (props) => (
          <Link to={props.row.original.id} variant="primary" className={styles.cellContent}>
            <LastSyncCell timeInSeconds={props.cell.getValue() || 0} enabled={props.row.original.enabled} />
          </Link>
        ),
        sortUndefined: 1,
      }),
      columnHelper.accessor("connectionJobStatuses", {
        header: () => <FormattedMessage id="sources.status" />,
        id: "status",
        meta: {
          noPadding: true,
          tdClassName: styles.statusIcons,
        },
        cell: (props) => (
          <Link to={props.row.original.id} variant="primary" className={styles.cellContent}>
            <AllConnectionsStatusCell statuses={props.cell.getValue()} />
          </Link>
        ),
        enableSorting: false,
      }),
      columnHelper.accessor("breakingChanges", {
        header: () => null,
        id: "breakingChanges",
        meta: {
          noPadding: true,
        },
        cell: (props) => {
          if (props.row.original.supportState != null && shouldDisplayBreakingChangeBanner(props.row.original)) {
            const { errorMessageId, errorType } = getBreakingChangeErrorMessage(
              props.row.original as Parameters<typeof getBreakingChangeErrorMessage>[0],
              connectorBreakingChangeDeadlinesEnabled
            );
            return (
              <Tooltip
                placement="bottom"
                control={
                  <Link to={props.row.original.id} variant="primary">
                    <Icon
                      size="sm"
                      type={errorType === "warning" ? "infoFilled" : "statusWarning"}
                      color={errorType === "warning" ? "warning" : "error"}
                    />
                  </Link>
                }
              >
                <FormattedMessage
                  id={errorMessageId}
                  values={{
                    actor_name: props.row.original.actorName,
                    actor_definition_name: props.row.original.actorDefinitionName,
                    actor_type: props.row.original.actorType,
                    upgrade_deadline: getHumanReadableUpgradeDeadline(props.row.original),
                  }}
                />
              </Tooltip>
            );
          }
          return null;
        },
        enableSorting: false,
      }),
    ],
    [connectorBreakingChangeDeadlinesEnabled]
  );

  const customScrollParent = useContext(ScrollParentContext);

  return (
    <Table
      rowId={(item: ActorTableDataItem) => item.id}
      columns={columns}
      data={tableData}
      testId={isSourceReadList(actorReadList) ? `sourcesTable` : `destinationsTable`}
      variant="white"
      initialSortBy={[{ id: "connectorName", desc: false }]}
      manualSorting
      sortingState={sortKey ? [sortingKeyToColumnId[sortKey]] : []}
      onSortingChange={(sortingUpdater) => {
        if (typeof sortingUpdater === "function") {
          const newSortingState = sortKey ? sortingUpdater([sortingKeyToColumnId[sortKey]]) : [];
          const column = getColumnFromSortingState(newSortingState);
          const direction = newSortingState[0]?.desc ? "desc" : "asc";
          setSortKey?.(`${column}_${direction}`);
        }
      }}
      showEmptyPlaceholder={false}
      virtualized={!!customScrollParent}
      virtualizedProps={{
        customScrollParent: customScrollParent ?? undefined,
        endReached: () => {
          if (hasNextPage && fetchNextPage) {
            fetchNextPage();
          }
        },
      }}
    />
  );
};
