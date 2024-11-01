import { createColumnHelper } from "@tanstack/react-table";
import React, { useContext } from "react";
import { FormattedMessage } from "react-intl";

import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Table } from "components/ui/Table";
import { Tooltip } from "components/ui/Tooltip";

import { getHumanReadableUpgradeDeadline, shouldDisplayBreakingChangeBanner } from "core/domain/connector";
import { FeatureItem, useFeature } from "core/services/features";
import { getBreakingChangeErrorMessage } from "pages/connections/StreamStatusPage/ConnectionStatusMessages";

import AllConnectionsStatusCell from "./components/AllConnectionsStatusCell";
import ConnectEntitiesCell from "./components/ConnectEntitiesCell";
import { ConnectorName } from "./components/ConnectorName";
import { EntityNameCell } from "./components/EntityNameCell";
import { LastSync } from "./components/LastSync";
import styles from "./ImplementationTable.module.scss";
import { EntityTableDataItem } from "./types";
import { ScrollParentContext } from "../ui/ScrollParent";

interface ImplementationTableProps {
  data: EntityTableDataItem[];
  entity: "source" | "destination";
  emptyPlaceholder?: React.ReactElement;
}

const ImplementationTable: React.FC<ImplementationTableProps> = ({ data, entity, emptyPlaceholder }) => {
  const columnHelper = createColumnHelper<EntityTableDataItem>();
  const connectorBreakingChangeDeadlinesEnabled = useFeature(FeatureItem.ConnectorBreakingChangeDeadlines);

  const columns = React.useMemo(
    () => [
      columnHelper.accessor("entityName", {
        header: () => <FormattedMessage id="tables.name" />,
        meta: {
          thClassName: styles.thEntityName,
          noPadding: true,
          responsive: true,
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
            <ConnectorName
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
            <LastSync timeInSeconds={props.cell.getValue() || 0} enabled={props.row.original.enabled} />
          </Link>
        ),
        sortUndefined: 1,
      }),
      columnHelper.accessor("connectEntities", {
        header: () => <FormattedMessage id="sources.status" />,
        id: "status",
        meta: {
          noPadding: true,
          tdClassName: styles.statusIcons,
        },
        cell: (props) => (
          <Link to={props.row.original.entityId} variant="primary" className={styles.cellContent}>
            <AllConnectionsStatusCell connectEntities={props.cell.getValue()} />
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
                  <Link to={props.row.original.entityId} variant="primary">
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
                    actor_name: props.row.original.entityName,
                    actor_definition_name: props.row.original.connectorName,
                    actor_type: entity,
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
    [columnHelper, entity, connectorBreakingChangeDeadlinesEnabled]
  );

  const customScrollParent = useContext(ScrollParentContext);

  return (
    <Table
      rowId="entityId"
      columns={columns}
      data={data}
      testId={`${entity}sTable`}
      initialSortBy={[{ id: "connectorName", desc: false }]}
      variant="white"
      customEmptyPlaceholder={emptyPlaceholder}
      virtualized={!!customScrollParent}
      virtualizedProps={{
        customScrollParent: customScrollParent ?? undefined,
      }}
    />
  );
};

export default ImplementationTable;
