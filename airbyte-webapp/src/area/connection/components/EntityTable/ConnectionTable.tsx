import { CellContext, ColumnDefTemplate, ColumnSort, createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import React, { useContext, useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Link } from "components/ui/Link";
import { ScrollParentContext } from "components/ui/ScrollParent";
import { Table } from "components/ui/Table";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useConnectionSemanticSearchAvailability } from "cloud/components/AgentsOptIn";
import { WebBackendConnectionListSortKey } from "core/api/types/AirbyteClient";
import { RoutePaths } from "pages/routePaths";

import { ConnectionStatus, ConnectionStatusIcon } from "./components/ConnectionStatus";
import { ConnectorName } from "./components/ConnectorName";
import { EntityNameCell as ConnectionNameText } from "./components/EntityNameCell";
import { EntityWarningsCell } from "./components/EntityWarningsCell";
import { FrequencyCell } from "./components/FrequencyCell";
import { LastSyncCell } from "./components/LastSyncCell";
import { SemanticSearchCell, useOrganizationSemanticSearchState } from "./components/SemanticSearchCell";
import { StateSwitchCell } from "./components/StateSwitchCell";
import { TagsCell } from "./components/TagsCell";
import styles from "./ConnectionTable.module.scss";
import { ConnectionTableDataItem } from "./types";

interface ConnectionTableProps {
  data: ConnectionTableDataItem[];
  entity: "source" | "destination" | "connection";
  variant?: React.ComponentProps<typeof Table>["variant"];
  sortKey?: WebBackendConnectionListSortKey;
  setSortKey?: (sortState: WebBackendConnectionListSortKey) => void;
  hasNextPage?: boolean;
  fetchNextPage?: () => void;
  isConnectionsListPage?: boolean;
}

const ConnectionTable: React.FC<ConnectionTableProps> = ({
  data,
  entity,
  variant,
  hasNextPage,
  fetchNextPage,
  setSortKey,
  sortKey,
  isConnectionsListPage = false,
}) => {
  const createLink = useCurrentWorkspaceLink();
  const isSemanticSearchAvailable = useConnectionSemanticSearchAvailability() === "available";
  const showSemanticSearchColumn = isConnectionsListPage && isSemanticSearchAvailable;
  // Read once for all rows, so rows that scroll into view do not read it again.
  const organizationSemanticSearch = useOrganizationSemanticSearchState({ enabled: showSemanticSearchColumn });

  const columnHelper = createColumnHelper<ConnectionTableDataItem>();

  const EntityNameCell = useMemo<ColumnDefTemplate<CellContext<ConnectionTableDataItem, string>>>(
    () =>
      // eslint-disable-next-line react/function-component-definition -- using function as it provides the component's DisplayName
      function EntityNameCell(props) {
        return (
          <Link
            to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
            variant="primary"
            className={styles.cellContent}
          >
            <ConnectorName
              value={props.cell.getValue()}
              actualName={props.row.original.connection.source?.sourceName ?? ""}
              icon={props.row.original.entityIcon}
              enabled={props.row.original.enabled}
              hideIcon={entity !== "connection"}
            />
          </Link>
        );
      },
    [createLink, entity]
  );

  const SemanticSearchCellWithLink = useMemo<ColumnDefTemplate<CellContext<ConnectionTableDataItem, unknown>>>(
    () =>
      // eslint-disable-next-line react/function-component-definition -- using function as it provides the component's DisplayName
      function SemanticSearchCellWithLink(props) {
        return (
          <Link
            to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
            variant="primary"
            className={styles.cellContent}
          >
            <SemanticSearchCell
              connection={props.row.original.connection}
              organizationSemanticSearch={organizationSemanticSearch}
            />
          </Link>
        );
      },
    [createLink, organizationSemanticSearch]
  );

  const columns = React.useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="tables.name" />,
        meta: {
          thClassName: styles.connectionName,
          responsive: true,
          noPadding: true,
        },
        cell: isConnectionsListPage ? NameWithoutStatusCell : NameCell,
      }),
      columnHelper.accessor("entityName", {
        header: () => (
          <FormattedMessage
            id={entity === "connection" ? "tables.destinationConnectionToName" : `tables.${entity}ConnectionToName`}
          />
        ),
        meta: {
          thClassName: styles.width25,
          responsive: true,
          noPadding: true,
        },
        cell: EntityNameCell,
      }),
      columnHelper.accessor("connectorName", {
        header: () => (
          <FormattedMessage id={entity === "connection" ? "tables.sourceConnectionToName" : "tables.connector"} />
        ),
        meta: {
          thClassName: styles.width25,
          responsive: true,
          noPadding: true,
        },
        cell: ConnectorNameCell,
      }),
      columnHelper.accessor("scheduleData", {
        header: () => <FormattedMessage id="tables.frequency" />,
        enableSorting: false,
        meta: {
          noPadding: true,
        },
        cell: FrequencyCell,
      }),
      columnHelper.accessor("tags", {
        header: () => <FormattedMessage id="connection.tags.title" />,
        enableSorting: false,
        meta: {
          noPadding: true,
          thClassName: styles.tags,
        },
        cell: TagsCell,
      }),
      columnHelper.accessor("lastSync", {
        header: () => <FormattedMessage id="tables.lastSync" />,
        cell: LastSyncCellWithLink,
        meta: {
          thClassName: styles.lastSync,
          noPadding: true,
        },
      }),
      ...(isConnectionsListPage
        ? [
            columnHelper.accessor("lastSyncStatus", {
              header: () => <FormattedMessage id="tables.syncStatus" />,
              enableSorting: false,
              meta: {
                thClassName: styles.iconColumn,
                noPadding: true,
              },
              cell: SyncStatusCell,
            }),
          ]
        : []),
      ...(showSemanticSearchColumn
        ? [
            columnHelper.display({
              id: "semanticSearch",
              header: () => <FormattedMessage id="tables.semanticSearch" />,
              enableSorting: false,
              meta: {
                thClassName: styles.iconColumn,
                noPadding: true,
              },
              cell: SemanticSearchCellWithLink,
            }),
          ]
        : []),
      columnHelper.accessor("enabled", {
        header: () => <FormattedMessage id="tables.enabled" />,
        meta: {
          thClassName: styles.enabled,
        },
        cell: StateSwitchCell,
        enableSorting: false,
      }),
      columnHelper.accessor("connection", {
        header: "",
        meta: {
          thClassName: styles.connectionSettings,
        },
        cell: EntityWarningsCell,
        enableSorting: false,
      }),
    ],
    [columnHelper, EntityNameCell, SemanticSearchCellWithLink, entity, isConnectionsListPage, showSemanticSearchColumn]
  );

  const sortingKeyToColumnId: Record<WebBackendConnectionListSortKey, ColumnSort> = {
    connectionName_asc: { id: "name", desc: false },
    connectionName_desc: { id: "name", desc: true },
    sourceName_asc: { id: "entityName", desc: false },
    sourceName_desc: { id: "entityName", desc: true },
    destinationName_asc: { id: "connectorName", desc: false },
    destinationName_desc: { id: "connectorName", desc: true },
    lastSync_asc: { id: "lastSync", desc: false },
    lastSync_desc: { id: "lastSync", desc: true },
  };

  const columnIdToSortingKey: Record<string, "connectionName" | "sourceName" | "destinationName" | "lastSync"> = {
    name: "connectionName",
    entityName: "sourceName",
    connectorName: "destinationName",
    lastSync: "lastSync",
  };

  const customScrollParent = useContext(ScrollParentContext);
  return (
    <Table
      rowId="connectionId"
      variant={variant}
      columns={columns}
      data={data}
      testId="connectionsTable"
      className={classNames(styles.connectionsTable, {
        [styles.nameWithStatusIcon]: !isConnectionsListPage,
      })}
      manualSorting
      sortingState={sortKey ? [sortingKeyToColumnId[sortKey]] : []}
      onSortingChange={(sortingUpdater) => {
        if (typeof sortingUpdater === "function") {
          const newSortingState = sortKey ? sortingUpdater([sortingKeyToColumnId[sortKey]]) : [];
          const column = columnIdToSortingKey[newSortingState[0]?.id];
          const direction = newSortingState[0]?.desc ? "desc" : "asc";
          if (column && direction) {
            setSortKey?.(`${column}_${direction}`);
          }
        }
      }}
      showEmptyPlaceholder={false}
      virtualized={!!customScrollParent}
      virtualizedProps={{
        overscan: 250,
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

export default ConnectionTable;

const NameCell: ColumnDefTemplate<CellContext<ConnectionTableDataItem, string>> = (props) => {
  const createLink = useCurrentWorkspaceLink();
  return (
    <Link
      to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
      variant="primary"
      className={styles.cellContent}
    >
      <ConnectionStatus
        status={props.row.original.lastSyncStatus}
        value={props.cell.getValue()}
        enabled={props.row.original.enabled}
      />
    </Link>
  );
};

const NameWithoutStatusCell: ColumnDefTemplate<CellContext<ConnectionTableDataItem, string>> = (props) => {
  const createLink = useCurrentWorkspaceLink();
  return (
    <Link
      to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
      variant="primary"
      className={styles.cellContent}
    >
      <ConnectionNameText value={props.cell.getValue()} enabled={props.row.original.enabled} />
    </Link>
  );
};

const SyncStatusCell: ColumnDefTemplate<CellContext<ConnectionTableDataItem, string | null>> = (props) => {
  const createLink = useCurrentWorkspaceLink();
  return (
    <Link
      to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
      variant="primary"
      className={styles.cellContent}
    >
      <ConnectionStatusIcon status={props.cell.getValue()} />
    </Link>
  );
};

const ConnectorNameCell: ColumnDefTemplate<CellContext<ConnectionTableDataItem, string>> = (props) => {
  const createLink = useCurrentWorkspaceLink();
  return (
    <Link
      to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
      variant="primary"
      className={styles.cellContent}
    >
      <ConnectorName
        value={props.cell.getValue()}
        actualName={props.row.original.connection.destination?.destinationName ?? ""}
        icon={props.row.original.connectorIcon}
        enabled={props.row.original.enabled}
      />
    </Link>
  );
};

const LastSyncCellWithLink: ColumnDefTemplate<CellContext<ConnectionTableDataItem, number | null | undefined>> = (
  props
) => {
  const createLink = useCurrentWorkspaceLink();
  return (
    <Link
      to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
      variant="primary"
      className={styles.cellContent}
    >
      <LastSyncCell timeInSeconds={props.cell.getValue()} enabled={props.row.original.enabled} />
    </Link>
  );
};
