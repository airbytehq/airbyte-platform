import { CellContext, ColumnDefTemplate, createColumnHelper } from "@tanstack/react-table";
import React, { useContext, useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Link } from "components/ui/Link";
import { ScrollParentContext } from "components/ui/ScrollParent";
import { Table } from "components/ui/Table";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { RoutePaths } from "pages/routePaths";

import { ConnectionStatus } from "./components/ConnectionStatus";
import { ConnectorName } from "./components/ConnectorName";
import { EntityWarningsCell } from "./components/EntityWarningsCell";
import { FrequencyCell } from "./components/FrequencyCell";
import { LastSync } from "./components/LastSync";
import { StateSwitchCell } from "./components/StateSwitchCell";
import { StreamsStatusCell } from "./components/StreamStatusCell";
import styles from "./ConnectionTable.module.scss";
import { ConnectionTableDataItem } from "./types";

interface ConnectionTableProps {
  data: ConnectionTableDataItem[];
  entity: "source" | "destination" | "connection";
  variant?: React.ComponentProps<typeof Table>["variant"];
}

const ConnectionTable: React.FC<ConnectionTableProps> = ({ data, entity, variant }) => {
  const createLink = useCurrentWorkspaceLink();
  const streamCentricUIEnabled = false;

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

  const columns = React.useMemo(
    () => [
      columnHelper.display({
        id: "stream-status",
        cell: StreamsStatusCell,
        size: 170,
      }),
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="tables.name" />,
        meta: {
          thClassName: styles.width30,
          responsive: true,
          noPadding: true,
        },
        cell: NameCell,
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("entityName", {
        header: () => (
          <FormattedMessage
            id={entity === "connection" ? "tables.destinationConnectionToName" : `tables.${entity}ConnectionToName`}
          />
        ),
        meta: {
          thClassName: styles.width30,
          responsive: true,
          noPadding: true,
        },
        cell: EntityNameCell,
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("connectorName", {
        header: () => (
          <FormattedMessage id={entity === "connection" ? "tables.sourceConnectionToName" : "tables.connector"} />
        ),
        meta: {
          thClassName: styles.width30,
          responsive: true,
          noPadding: true,
        },
        cell: ConnectorNameCell,
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("scheduleData", {
        header: () => <FormattedMessage id="tables.frequency" />,
        enableSorting: false,
        meta: {
          noPadding: true,
        },
        cell: FrequencyCell,
      }),
      columnHelper.accessor("lastSync", {
        header: () => <FormattedMessage id="tables.lastSync" />,
        cell: LastSyncCell,
        meta: {
          thClassName: styles.width20,
          noPadding: true,
        },
        sortUndefined: 1,
      }),
      columnHelper.accessor("enabled", {
        header: () => <FormattedMessage id="tables.enabled" />,
        meta: {
          thClassName: styles.thEnabled,
        },
        cell: StateSwitchCell,
        enableSorting: false,
      }),
      columnHelper.accessor("connection", {
        header: "",
        meta: {
          thClassName: styles.thConnectionSettings,
        },
        cell: EntityWarningsCell,
        enableSorting: false,
      }),
    ],
    [columnHelper, entity, EntityNameCell]
  );

  const customScrollParent = useContext(ScrollParentContext);
  return (
    <Table
      rowId="connectionId"
      variant={variant}
      columns={columns}
      data={data}
      testId="connectionsTable"
      columnVisibility={{ "stream-status": streamCentricUIEnabled }}
      className={styles.connectionsTable}
      initialSortBy={[{ id: "entityName", desc: false }]}
      virtualized={!!customScrollParent}
      virtualizedProps={{
        customScrollParent: customScrollParent ?? undefined,
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

const LastSyncCell: ColumnDefTemplate<CellContext<ConnectionTableDataItem, number | null | undefined>> = (props) => {
  const createLink = useCurrentWorkspaceLink();
  return (
    <Link
      to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
      variant="primary"
      className={styles.cellContent}
    >
      <LastSync timeInSeconds={props.cell.getValue()} enabled={props.row.original.enabled} />
    </Link>
  );
};
