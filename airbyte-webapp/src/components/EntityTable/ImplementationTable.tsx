import { createColumnHelper } from "@tanstack/react-table";
import React from "react";
import { FormattedMessage } from "react-intl";

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
  onClickRow?: (data: EntityTableDataItem) => void;
}

const ImplementationTable: React.FC<IProps> = ({ data, entity, onClickRow }) => {
  const columnHelper = createColumnHelper<EntityTableDataItem>();

  const columns = React.useMemo(
    () => [
      columnHelper.accessor("entityName", {
        header: () => <FormattedMessage id="tables.name" />,
        meta: {
          thClassName: styles.thEntityName,
        },
        cell: (props) => <EntityNameCell value={props.cell.getValue()} enabled={props.row.original.enabled} />,
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("connectorName", {
        header: () => <FormattedMessage id="tables.connector" />,
        cell: (props) => (
          <ConnectorNameCell
            value={props.cell.getValue()}
            icon={props.row.original.connectorIcon}
            enabled={props.row.original.enabled}
          />
        ),
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("connectEntities", {
        header: () => <FormattedMessage id={`tables.${entity}ConnectWith`} />,
        cell: (props) => (
          <ConnectEntitiesCell values={props.cell.getValue()} entity={entity} enabled={props.row.original.enabled} />
        ),
        enableSorting: false,
      }),
      columnHelper.accessor("lastSync", {
        header: () => <FormattedMessage id="tables.lastSync" />,
        cell: (props) => (
          <LastSyncCell timeInSeconds={props.cell.getValue() || 0} enabled={props.row.original.enabled} />
        ),
        sortUndefined: 1,
      }),
      columnHelper.accessor("connectEntities", {
        header: () => <FormattedMessage id="sources.status" />,
        id: "status",
        cell: (props) => <AllConnectionsStatusCell connectEntities={props.cell.getValue()} />,
        enableSorting: false,
      }),
    ],
    [columnHelper, entity]
  );

  return (
    <Table
      columns={columns}
      data={data}
      onClickRow={onClickRow}
      testId={`${entity}sTable`}
      initialSortBy={[{ id: "connectorName", desc: false }]}
    />
  );
};

export default ImplementationTable;
