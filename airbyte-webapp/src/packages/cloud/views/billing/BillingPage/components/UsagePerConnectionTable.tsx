import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import queryString from "query-string";
import React, { useCallback } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { SortOrderEnum } from "components/EntityTable/types";
import { ArrowRightIcon } from "components/icons/ArrowRightIcon";
import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { Table } from "components/ui/Table";
import { SortableTableHeader } from "components/ui/Table";
import { Text } from "components/ui/Text";

import { ConnectionScheduleType } from "core/request/AirbyteClient";
import { useQuery } from "hooks/useQuery";
import { RoutePaths } from "pages/routePaths";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

import styles from "./UsagePerConnectionTable.module.scss";
import { UsagePerDayGraph } from "./UsagePerDayGraph";
import { ConnectionFreeAndPaidUsage, useCreditsUsage } from "./useCreditsUsage";
import { BillingPageQueryParams } from "../BillingPage";

export const UsagePerConnectionTable: React.FC = () => {
  const { workspaceId } = useCurrentWorkspace();

  const query = useQuery<BillingPageQueryParams>();

  const navigate = useNavigate();

  const { freeAndPaidUsageByConnection, formatCredits } = useCreditsUsage();
  const sortBy = query.sortBy || "connectionName";
  const sortOrder = query.order || SortOrderEnum.ASC;

  const onSortClick = useCallback(
    (field: string) => {
      const order =
        sortBy !== field ? SortOrderEnum.ASC : sortOrder === SortOrderEnum.ASC ? SortOrderEnum.DESC : SortOrderEnum.ASC;
      navigate({
        search: queryString.stringify(
          {
            sortBy: field,
            order,
          },
          { skipNull: true }
        ),
      });
    },
    [navigate, sortBy, sortOrder]
  );

  const sortData = useCallback(
    (a, b) => {
      let result;
      if (sortBy === "totalUsage") {
        result = a.totalUsage - b.totalUsage;
      } else {
        result = a[sortBy].toLowerCase().localeCompare(b[sortBy].toLowerCase());
      }
      if (sortOrder === SortOrderEnum.DESC) {
        return -1 * result;
      }

      return result;
    },
    [sortBy, sortOrder]
  );

  const sortingData = React.useMemo(
    () => freeAndPaidUsageByConnection.sort(sortData),
    [sortData, freeAndPaidUsageByConnection]
  );

  const columnHelper = createColumnHelper<ConnectionFreeAndPaidUsage>();

  const billingInsightsColumns = React.useMemo(() => {
    return [
      columnHelper.accessor("connection.connectionName", {
        header: () => (
          <SortableTableHeader
            onClick={() => onSortClick("connection_connectionName")}
            isActive={sortBy === "connection_connectionName"}
            isAscending={sortOrder === SortOrderEnum.ASC}
          >
            <FormattedMessage id="credits.connection" />
          </SortableTableHeader>
        ),
        meta: {
          responsive: true,
        },
        cell: (props) => (
          <Link
            to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${props.row.original.connection.connectionId}`}
          >
            <Text size="sm" className={styles.cellText}>
              {props.cell.getValue()}
            </Text>
          </Link>
        ),
      }),
      columnHelper.accessor("connection.sourceConnectionName", {
        header: () => (
          <SortableTableHeader
            onClick={() => onSortClick("connection_sourceConnectionName")}
            isActive={sortBy === "connection_sourceConnectionName"}
            isAscending={sortOrder === SortOrderEnum.ASC}
          >
            <FormattedMessage id="credits.source" />
          </SortableTableHeader>
        ),
        meta: {
          responsive: true,
        },
        cell: (props) => (
          <Link
            to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Source}/${props.row.original.connection.sourceId}`}
          >
            <FlexContainer direction="row" alignItems="center">
              <ConnectorIcon icon={props.row.original.connection.sourceIcon} />
              <Text size="sm" className={styles.cellText}>
                {props.cell.getValue()}
              </Text>
            </FlexContainer>
          </Link>
        ),
      }),
      columnHelper.display({
        id: "arrow",
        cell: () => <ArrowRightIcon />,
        meta: {
          thClassName: classNames(styles.header, styles["header--light"]),
          responsive: true,
        },
      }),
      columnHelper.accessor("connection.destinationConnectionName", {
        header: () => (
          <SortableTableHeader
            onClick={() => onSortClick("connection_destinationConnectionName")}
            isActive={sortBy === "connection_destinationConnectionName"}
            isAscending={sortOrder === SortOrderEnum.ASC}
          >
            <FormattedMessage id="credits.destination" />
          </SortableTableHeader>
        ),
        meta: {
          responsive: true,
        },
        cell: (props) => (
          <Link
            to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Destination}/${props.row.original.connection.destinationId}`}
          >
            <FlexContainer direction="row" alignItems="center">
              <ConnectorIcon icon={props.row.original.connection.destinationIcon} />
              <Text size="sm">{props.cell.getValue()}</Text>
            </FlexContainer>
          </Link>
        ),
      }),
      columnHelper.display({
        id: "schedule",

        header: () => <FormattedMessage id="credits.schedule" />,
        cell: (props) => (
          <FlexContainer className={styles.cell} alignItems="center">
            <Text size="sm" className={styles.cellText}>
              {props.row.original.connection.connectionScheduleType ===
              (ConnectionScheduleType.manual || ConnectionScheduleType.cron) ? (
                <FormattedMessage id={`frequency.${props.row.original.connection.connectionScheduleType}`} />
              ) : (
                <FormattedMessage
                  id={`frequency.${props.row.original.connection.connectionScheduleTimeUnit ?? "manual"}`}
                  values={{ value: props.row.original.connection.connectionScheduleUnits }}
                />
              )}
            </Text>
          </FlexContainer>
        ),
        meta: {
          thClassName: classNames(styles["header--nonSortable"]),
          responsive: true,
        },
      }),
      columnHelper.accessor("totalUsage", {
        header: () => (
          <SortableTableHeader
            onClick={() => onSortClick("totalUsage")}
            isActive={sortBy === "totalUsage"}
            isAscending={sortOrder === SortOrderEnum.ASC}
          >
            <FormattedMessage id="credits.usage" />
          </SortableTableHeader>
        ),
        meta: {
          responsive: true,
        },
        cell: (props) => (
          <FlexContainer alignItems="center">
            <UsagePerDayGraph chartData={props.row.original.usage} minimized />
            <FlexContainer direction="column" gap="none">
              {props.row.original.totalFreeUsage > 0 && (
                <Text className={classNames(styles.usageValue, styles["usageValue--green"])} size="sm">
                  {formatCredits(props.row.original.totalFreeUsage)}
                </Text>
              )}
              <Text className={styles.usageValue} size="sm">
                {formatCredits(props.row.original.totalBilledCost)}
              </Text>
            </FlexContainer>
          </FlexContainer>
        ),
      }),
    ];
  }, [columnHelper, formatCredits, onSortClick, sortBy, sortOrder, workspaceId]);

  return (
    <div className={styles.content}>
      <Table variant="transparent" columns={billingInsightsColumns} data={sortingData} sortedByColumn={sortBy} />
    </div>
  );
};
