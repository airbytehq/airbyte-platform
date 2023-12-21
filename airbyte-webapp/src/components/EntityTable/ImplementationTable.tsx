import { createColumnHelper } from "@tanstack/react-table";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Link } from "components/ui/Link";
import { Table } from "components/ui/Table";

import AllConnectionsStatusCell from "./components/AllConnectionsStatusCell";
import ConnectEntitiesCell from "./components/ConnectEntitiesCell";
import { ConnectorNameCell } from "./components/ConnectorNameCell";
import { EntityNameCell } from "./components/EntityNameCell";
import { LastSyncCell } from "./components/LastSyncCell";
import styles from "./ImplementationTable.module.scss";
import { EntityTableDataItem } from "./types";

interface IProps {
  data: EntityTableDataItem[];
  entity: "source" | "destination";
}

const ImplementationTable: React.FC<IProps> = ({ data, entity }) => {
  const columnHelper = createColumnHelper<EntityTableDataItem>();

  const columns = React.useMemo(
    () => [
      columnHelper.accessor("entityName", {
        header: () => <FormattedMessage id="tables.name" />,
        meta: {
          thClassName: styles.thEntityName,
          noPadding: true,
        },
        cell: (props) => (
          <Link to={props.row.original.entityId} variant="primary" className={styles.cellContent}>
            <EntityNameCell value={props.cell.getValue()} enabled={props.row.original.enabled} />
          </Link>
        ),
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("connectorName", {
        header: () => <FormattedMessage id="tables.connector" />,
        meta: {
          noPadding: true,
          thClassName: styles.thConnectorName,
          responsive: true,
        },
        cell: (props) => (
          <Link to={props.row.original.entityId} variant="primary" className={styles.cellContent}>
            <ConnectorNameCell
              value={props.cell.getValue()}
              icon={props.row.original.connectorIcon}
              enabled={props.row.original.enabled}
            />
          </Link>
        ),
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("connectEntities", {
        header: () => <FormattedMessage id={`tables.${entity}ConnectWith`} />,
        meta: {
          noPadding: true,
          thClassName: styles.thConnectEntities,
          responsive: true,
        },
        cell: (props) => (
          <Link to={props.row.original.entityId} variant="primary" className={styles.cellContent}>
            <ConnectEntitiesCell values={props.cell.getValue()} entity={entity} enabled={props.row.original.enabled} />
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
          <Link to={props.row.original.entityId} variant="primary" className={styles.cellContent}>
            <LastSyncCell timeInSeconds={props.cell.getValue() || 0} enabled={props.row.original.enabled} />
          </Link>
        ),
        sortUndefined: 1,
      }),
      columnHelper.accessor("connectEntities", {
        header: () => <FormattedMessage id="sources.status" />,
        id: "status",
        meta: {
          noPadding: true,
        },
        cell: (props) => (
          <Link to={props.row.original.entityId} variant="primary" className={styles.cellContent}>
            <AllConnectionsStatusCell connectEntities={props.cell.getValue()} />
          </Link>
        ),
        enableSorting: false,
      }),
    ],
    [columnHelper, entity]
  );

  return (
    <Table
      columns={columns}
      data={data}
      testId={`${entity}sTable`}
      initialSortBy={[{ id: "connectorName", desc: false }]}
    />
  );
};

export default ImplementationTable;
