import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import queryString from "query-string";
import React, { useCallback, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { SortOrderEnum } from "components/EntityTable/types";
import { ArrowRightIcon } from "components/icons/ArrowRightIcon";
import { ReleaseStageBadge } from "components/ReleaseStageBadge";
import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { Table } from "components/ui/Table";
import { SortableTableHeader } from "components/ui/Table";
import { TextWithOverflowTooltip } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { ConnectionScheduleType, ConnectionStatus } from "core/request/AirbyteClient";
import { useQuery } from "hooks/useQuery";
import { RoutePaths } from "pages/routePaths";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

import { ConnectionFreeAndPaidUsage } from "./calculateUsageDataObjects";
import { useCreditsContext } from "./CreditsUsageContext";
import { FormattedCredits } from "./FormattedCredits";
import styles from "./UsagePerConnectionTable.module.scss";
import { UsagePerDayGraph } from "./UsagePerDayGraph";
import { BillingPageQueryParams } from "../BillingPage";

export const UsagePerConnectionTable: React.FC = () => {
  const { workspaceId } = useCurrentWorkspace();

  const query = useQuery<BillingPageQueryParams>();

  const navigate = useNavigate();

  const { freeAndPaidUsageByConnection } = useCreditsContext();
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
        result = a.connection[sortBy].toLowerCase().localeCompare(b.connection[sortBy].toLowerCase());
      }
      if (sortOrder === SortOrderEnum.DESC) {
        return -1 * result;
      }

      return result;
    },
    [sortBy, sortOrder]
  );

  const sortingData = React.useMemo(
    // It is important that we return this way so that it creates a whole new array
    // otherwise, ReactTable is unable to recognize that it is a new array and sorting will break.
    // And it will only break in the minified version (it will look fine in dev mode).
    () => [...freeAndPaidUsageByConnection.sort(sortData)],
    [sortData, freeAndPaidUsageByConnection]
  );

  const columnHelper = useMemo(() => createColumnHelper<ConnectionFreeAndPaidUsage>(), []);

  const billingInsightsColumns = React.useMemo(() => {
    return [
      columnHelper.accessor("connection.connectionName", {
        header: () => (
          <SortableTableHeader
            onClick={() => onSortClick("connectionName")}
            isActive={sortBy === "connectionName"}
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
            <FlexContainer alignItems="center" gap="xs">
              <TextWithOverflowTooltip
                size="sm"
                color={props.row.original.connection.status === ConnectionStatus.deprecated ? "grey300" : undefined}
                className={classNames(styles.cellText)}
              >
                {props.cell.getValue()}
                {props.row.original.connection.status === ConnectionStatus.deprecated && (
                  <InfoTooltip>
                    <FormattedMessage id="credits.deleted" />
                  </InfoTooltip>
                )}
              </TextWithOverflowTooltip>
            </FlexContainer>
          </Link>
        ),
      }),
      columnHelper.accessor("connection.sourceConnectionName", {
        header: () => (
          <SortableTableHeader
            onClick={() => onSortClick("sourceConnectionName")}
            isActive={sortBy === "sourceConnectionName"}
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
            <FlexContainer
              direction="row"
              alignItems="center"
              className={classNames({
                [styles.deleted]: props.row.original.connection.status === ConnectionStatus.deprecated,
              })}
            >
              <ConnectorIcon icon={props.row.original.connection.sourceIcon} />
              <TextWithOverflowTooltip size="sm" className={styles.cellText}>
                {props.cell.getValue()}
              </TextWithOverflowTooltip>
              <ReleaseStageBadge stage={props.row.original.connection.sourceReleaseStage} />
            </FlexContainer>
          </Link>
        ),
      }),
      columnHelper.display({
        id: "arrow",
        cell: (props) => (
          <div
            className={classNames({
              [styles.deleted]: props.row.original.connection.status === ConnectionStatus.deprecated,
            })}
          >
            <ArrowRightIcon />
          </div>
        ),
        meta: {
          thClassName: classNames(styles.header, styles["header--light"]),
          responsive: true,
        },
      }),
      columnHelper.accessor("connection.destinationConnectionName", {
        header: () => (
          <SortableTableHeader
            onClick={() => onSortClick("destinationConnectionName")}
            isActive={sortBy === "destinationConnectionName"}
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
            <FlexContainer
              direction="row"
              alignItems="center"
              className={classNames({
                [styles.deleted]: props.row.original.connection.status === ConnectionStatus.deprecated,
              })}
            >
              <ConnectorIcon icon={props.row.original.connection.destinationIcon} />
              <TextWithOverflowTooltip
                size="sm"
                color={props.row.original.connection.status === ConnectionStatus.deprecated ? "grey300" : undefined}
              >
                {props.cell.getValue()}
              </TextWithOverflowTooltip>
              <ReleaseStageBadge stage={props.row.original.connection.destinationReleaseStage} />
            </FlexContainer>
          </Link>
        ),
      }),
      columnHelper.display({
        id: "schedule",

        header: () => <FormattedMessage id="credits.schedule" />,
        cell: (props) => (
          <FlexContainer className={styles.cell} alignItems="center">
            <TextWithOverflowTooltip size="sm" className={styles.cellText}>
              {props.row.original.connection.connectionScheduleType ===
              (ConnectionScheduleType.manual || ConnectionScheduleType.cron) ? (
                <FormattedMessage id={`frequency.${props.row.original.connection.connectionScheduleType}`} />
              ) : (
                <FormattedMessage
                  id={`frequency.${props.row.original.connection.connectionScheduleTimeUnit ?? "manual"}`}
                  values={{ value: props.row.original.connection.connectionScheduleUnits }}
                />
              )}
            </TextWithOverflowTooltip>
          </FlexContainer>
        ),
        meta: {
          thClassName: styles["header--nonSortable"],
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
          <FlexContainer
            alignItems="center"
            className={classNames({
              [styles.deleted]: props.row.original.connection.status === ConnectionStatus.deprecated,
            })}
          >
            <UsagePerDayGraph chartData={props.row.original.usage} minimized />
            <FlexContainer direction="column" gap="none">
              {props.row.original.totalFreeUsage > 0 && (
                <FormattedCredits
                  credits={props.row.original.totalFreeUsage}
                  color={props.row.original.connection.status === ConnectionStatus.deprecated ? "grey300" : "green"}
                  size="sm"
                />
              )}
              {props.row.original.totalBilledCost > 0 && (
                <FormattedCredits
                  color={props.row.original.connection.status === ConnectionStatus.deprecated ? "grey300" : undefined}
                  credits={props.row.original.totalBilledCost}
                  size="sm"
                />
              )}
            </FlexContainer>
          </FlexContainer>
        ),
      }),
    ];
  }, [columnHelper, onSortClick, sortBy, sortOrder, workspaceId]);

  return (
    <div className={styles.content}>
      <Table
        variant="white"
        columns={billingInsightsColumns}
        sortedByColumn={sortBy === "totalUsage" ? "totalUsage" : `connection_${sortBy}`}
        data={sortingData}
      />
    </div>
  );
};
