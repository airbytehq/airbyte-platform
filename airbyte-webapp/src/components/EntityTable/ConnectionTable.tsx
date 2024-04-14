import { createColumnHelper } from "@tanstack/react-table";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Link } from "components/ui/Link";
import { Table } from "components/ui/Table";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { RoutePaths } from "pages/routePaths";

import { ConnectionStatusCell } from "./components/ConnectionStatusCell";
import { ConnectorNameCell } from "./components/ConnectorNameCell";
import { FrequencyCell } from "./components/FrequencyCell";
import { LastSyncCell } from "./components/LastSyncCell";
import { SchemaChangeCell } from "./components/SchemaChangeCell";
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
        cell: (props) => (
          <Link
            to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
            variant="primary"
            className={styles.cellContent}
          >
            <ConnectionStatusCell
              status={props.row.original.lastSyncStatus}
              value={props.cell.getValue()}
              enabled={props.row.original.enabled}
            />
          </Link>
        ),
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
        cell: (props) => (
          <Link
            to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
            variant="primary"
            className={styles.cellContent}
          >
            <ConnectorNameCell
              value={props.cell.getValue()}
              actualName={props.row.original.connection.source?.sourceName ?? ""}
              icon={props.row.original.entityIcon}
              enabled={props.row.original.enabled}
              hideIcon={entity !== "connection"}
            />
          </Link>
        ),
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
        cell: (props) => (
          <Link
            to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
            variant="primary"
            className={styles.cellContent}
          >
            <ConnectorNameCell
              value={props.cell.getValue()}
              actualName={props.row.original.connection.destination?.destinationName ?? ""}
              icon={props.row.original.connectorIcon}
              enabled={props.row.original.enabled}
            />
          </Link>
        ),
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("scheduleData", {
        header: () => <FormattedMessage id="tables.frequency" />,
        enableSorting: false,
        meta: {
          noPadding: true,
        },
        cell: (props) => (
          <Link
            to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
            variant="primary"
            className={styles.cellContent}
          >
            <FrequencyCell
              value={props.cell.getValue()}
              enabled={props.row.original.enabled}
              scheduleType={props.row.original.scheduleType}
            />
          </Link>
        ),
      }),
      columnHelper.accessor("lastSync", {
        header: () => <FormattedMessage id="tables.lastSync" />,
        cell: (props) => (
          <Link
            to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)}
            variant="primary"
            className={styles.cellContent}
          >
            <LastSyncCell timeInSeconds={props.cell.getValue()} enabled={props.row.original.enabled} />
          </Link>
        ),
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
        cell: (props) => (
          <StateSwitchCell
            connectionId={props.row.original.connectionId}
            enabled={props.cell.getValue()}
            schemaChange={props.row.original.schemaChange}
          />
        ),
        enableSorting: false,
      }),
      columnHelper.accessor("schemaChange", {
        header: "",
        meta: {
          thClassName: styles.thConnectionSettings,
        },
        cell: (props) => (
          <SchemaChangeCell
            connectionId={props.row.original.connectionId}
            schemaChange={props.row.original.schemaChange}
          />
        ),
        enableSorting: false,
      }),
    ],
    [columnHelper, createLink, entity]
  );

  return (
    <Table
      variant={variant}
      columns={columns}
      data={data}
      testId="connectionsTable"
      columnVisibility={{ "stream-status": streamCentricUIEnabled }}
      className={styles.connectionsTable}
      initialSortBy={[{ id: "entityName", desc: false }]}
    />
  );
};

export default ConnectionTable;
