import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import queryString from "query-string";
import React, { useCallback } from "react";
import { FormattedMessage, FormattedNumber } from "react-intl";
import { useNavigate } from "react-router-dom";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { SortOrderEnum } from "components/EntityTable/types";
import { ArrowRightIcon } from "components/icons/ArrowRightIcon";
import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { NextTable } from "components/ui/NextTable";
import { SortableTableHeader } from "components/ui/Table";
import { Text } from "components/ui/Text";

import { useExperiment } from "hooks/services/Experiment";
import { useQuery } from "hooks/useQuery";
import { CreditConsumptionByConnector } from "packages/cloud/lib/domain/cloudWorkspaces/types";
import { RoutePaths } from "pages/routePaths";
import { useDestinationDefinitionList } from "services/connector/DestinationDefinitionService";
import { useSourceDefinitionList } from "services/connector/SourceDefinitionService";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

import ConnectionCell from "./ConnectionCell";
import UsageCell from "./UsageCell";
import styles from "./UsagePerConnectionTable.module.scss";
import { BillingPageQueryParams } from "../BillingPage";

interface UsagePerConnectionTableProps {
  creditConsumption: CreditConsumptionByConnector[];
}

type FullTableProps = CreditConsumptionByConnector & {
  creditsConsumedPercent: number;
  sourceIcon?: string;
  destinationIcon?: string;
};

export const UsagePerConnectionTable: React.FC<UsagePerConnectionTableProps> = ({ creditConsumption }) => {
  const isBillingInsightsEnabled = useExperiment("billing.billingInsights", false);
  const { workspaceId } = useCurrentWorkspace();

  const query = useQuery<BillingPageQueryParams>();

  const navigate = useNavigate();
  const { sourceDefinitions } = useSourceDefinitionList();
  const { destinationDefinitions } = useDestinationDefinitionList();

  const creditConsumptionWithPercent = React.useMemo<FullTableProps[]>(() => {
    const sumCreditsConsumed = creditConsumption.reduce((a, b) => a + b.creditsConsumed, 0);
    return creditConsumption.map((item) => {
      const currentSourceDefinition = sourceDefinitions.find(
        (def) => def.sourceDefinitionId === item.sourceDefinitionId
      );
      const currentDestinationDefinition = destinationDefinitions.find(
        (def) => def.destinationDefinitionId === item.destinationDefinitionId
      );
      const newItem: FullTableProps = {
        ...item,
        sourceIcon: currentSourceDefinition?.icon,
        destinationIcon: currentDestinationDefinition?.icon,
        creditsConsumedPercent: sumCreditsConsumed ? (item.creditsConsumed / sumCreditsConsumed) * 100 : 0,
      };

      return newItem;
    });
  }, [creditConsumption, sourceDefinitions, destinationDefinitions]);

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
      if (sortBy === "usage") {
        result = a.creditsConsumed - b.creditsConsumed;
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
    // This is temporary solution, since there is an issue with array that
    // creditConsumptionWithPercent.sort(sortData) returns; when passed into useReactTable
    // the reference to this array stays the same, so useReactTable is not updating the table,
    // therefore sorting not working; after implementing native react table sorting mechanism
    // this problem should be solved
    () => [...creditConsumptionWithPercent.sort(sortData)],
    [sortData, creditConsumptionWithPercent]
  );

  const columnHelper = createColumnHelper<FullTableProps>();

  const billingInsightsColumns = React.useMemo(() => {
    return !isBillingInsightsEnabled
      ? null
      : [
          columnHelper.accessor("connectionName", {
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
              thClassName: classNames(styles.header, styles["header--light"], {
                [styles.headerSorted]: sortBy === "connectionName",
              }),
              responsive: true,
            },
            cell: (props) => (
              <Link
                to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${props.row.original.connectionId}`}
              >
                <Text size="sm" className={styles.cellText}>
                  {props.cell.getValue()}
                </Text>
              </Link>
            ),
          }),
          columnHelper.accessor("sourceConnectionName", {
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
              thClassName: classNames(styles.header, styles["header--light"], {
                [styles.headerSorted]: sortBy === "sourceConnectionName",
              }),
              responsive: true,
            },
            cell: (props) => (
              <Link to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Source}/${props.row.original.sourceId}`}>
                <FlexContainer direction="row" alignItems="center">
                  <ConnectorIcon icon={props.row.original.sourceIcon} />
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
          columnHelper.accessor("destinationConnectionName", {
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
              thClassName: classNames(styles.header, styles["header--light"], {
                [styles.headerSorted]: sortBy === "destinationConnectionName",
              }),
              responsive: true,
            },
            cell: (props) => (
              <Link
                to={`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Destination}/${props.row.original.destinationId}`}
              >
                <FlexContainer direction="row" alignItems="center">
                  <ConnectorIcon icon={props.row.original.destinationIcon} />
                  <Text size="sm">{props.cell.getValue()}</Text>
                </FlexContainer>
              </Link>
            ),
          }),
          columnHelper.display({
            id: "schedule",
            header: () => (
              <div className={styles.nonSortableHeader}>
                <FormattedMessage id="credits.schedule" />
              </div>
            ),
            cell: () => (
              <FlexContainer className={styles.cell} alignItems="center">
                <Text size="sm" className={styles.cellText}>
                  PLACEHOLDER!!
                </Text>
              </FlexContainer>
            ),
            meta: {
              thClassName: classNames(styles.header, styles["header--light"]),
              responsive: true,
            },
          }),
          columnHelper.accessor("creditsConsumedPercent", {
            header: () => (
              <SortableTableHeader
                onClick={() => onSortClick("usage")}
                isActive={sortBy === "usage"}
                isAscending={sortOrder === SortOrderEnum.ASC}
              >
                <FormattedMessage id="credits.usage" />
              </SortableTableHeader>
            ),
            meta: {
              thClassName: classNames(styles.header, styles["header--light"], {
                [styles.headerSorted]: sortBy === "usage",
              }),
              responsive: true,
            },
            cell: (props) => (
              <FlexContainer>
                <UsageCell percent={props.cell.getValue()} />
                <Text className={styles.usageValue} size="lg">
                  <FormattedNumber
                    value={props.row.original.creditsConsumed}
                    maximumFractionDigits={2}
                    minimumFractionDigits={2}
                  />
                </Text>
              </FlexContainer>
            ),
          }),
        ];
  }, [columnHelper, isBillingInsightsEnabled, onSortClick, sortBy, sortOrder, workspaceId]);

  const columns = React.useMemo(
    () => [
      columnHelper.accessor("sourceDefinitionName", {
        header: () => (
          <SortableTableHeader
            onClick={() => onSortClick("sourceDefinitionName")}
            isActive={sortBy === "sourceDefinitionName"}
            isAscending={sortOrder === SortOrderEnum.ASC}
          >
            <FormattedMessage id="credits.connection" />
          </SortableTableHeader>
        ),
        meta: {
          thClassName: classNames(styles.thConnection, styles.light),
        },
        cell: (props) => (
          <ConnectionCell
            sourceDefinitionName={props.cell.getValue()}
            destinationDefinitionName={props.row.original.destinationDefinitionName}
            sourceIcon={props.row.original.sourceIcon}
            destinationIcon={props.row.original.destinationIcon}
          />
        ),
      }),
      columnHelper.accessor("creditsConsumed", {
        header: () => (
          <SortableTableHeader
            onClick={() => onSortClick("usage")}
            isActive={sortBy === "usage"}
            isAscending={sortOrder === SortOrderEnum.ASC}
          >
            <FormattedMessage id="credits.usage" />
          </SortableTableHeader>
        ),
        meta: {
          thClassName: classNames(styles.thCreditsConsumed, styles.light),
        },
        cell: (props) => (
          <Text className={styles.usageValue} size="lg">
            <FormattedNumber value={props.cell.getValue()} maximumFractionDigits={2} minimumFractionDigits={2} />
          </Text>
        ),
      }),
      columnHelper.accessor("creditsConsumedPercent", {
        header: "",
        meta: {
          thClassName: classNames(styles.thCreditsConsumedPercent, styles.light),
        },
        cell: (props) => <UsageCell percent={props.cell.getValue()} />,
      }),
      columnHelper.accessor("connectionId", {
        header: "",
        cell: () => <div />,
        meta: {
          thClassName: classNames(styles.thConnectionId, styles.light),
        },
      }),
    ],
    [columnHelper, onSortClick, sortBy, sortOrder]
  );

  return (
    <div className={styles.content}>
      <NextTable columns={billingInsightsColumns ?? columns} data={sortingData} light />
    </div>
  );
};
