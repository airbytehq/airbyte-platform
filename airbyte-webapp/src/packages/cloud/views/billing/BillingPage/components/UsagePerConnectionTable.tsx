import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { SupportLevelBadge } from "components/ui/SupportLevelBadge";
import { Table } from "components/ui/Table";
import { TextWithOverflowTooltip } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { useCurrentWorkspace } from "core/api";
import { ConnectionScheduleType, ConnectionStatus } from "core/api/types/AirbyteClient";
import { RoutePaths } from "pages/routePaths";

import { ConnectionFreeAndPaidUsage } from "./calculateUsageDataObjects";
import { useCreditsContext } from "./CreditsUsageContext";
import { FormattedCredits } from "./FormattedCredits";
import styles from "./UsagePerConnectionTable.module.scss";
import { UsagePerDayGraph } from "./UsagePerDayGraph";

export const UsagePerConnectionTable: React.FC = () => {
  const { workspaceId } = useCurrentWorkspace();

  const { freeAndPaidUsageByConnection } = useCreditsContext();

  const columnHelper = useMemo(() => createColumnHelper<ConnectionFreeAndPaidUsage>(), []);

  const columns = React.useMemo(() => {
    return [
      columnHelper.accessor("connection.connectionName", {
        header: () => <FormattedMessage id="credits.connection" />,
        meta: {
          responsive: true,
        },
        sortingFn: "alphanumeric",
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
        header: () => <FormattedMessage id="credits.source" />,
        meta: {
          responsive: true,
        },
        sortingFn: "alphanumeric",
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
              <SupportLevelBadge
                supportLevel={props.row.original.connection.sourceSupportLevel}
                custom={props.row.original.connection.sourceCustom}
              />
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
            <Icon type="arrowRight" />
          </div>
        ),
        enableSorting: false,
        meta: {
          responsive: true,
        },
      }),
      columnHelper.accessor("connection.destinationConnectionName", {
        header: () => <FormattedMessage id="credits.destination" />,
        meta: {
          responsive: true,
        },
        sortingFn: "alphanumeric",
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
              <SupportLevelBadge
                supportLevel={props.row.original.connection.destinationSupportLevel}
                custom={props.row.original.connection.destinationCustom}
              />
            </FlexContainer>
          </Link>
        ),
      }),
      columnHelper.display({
        id: "schedule",
        header: () => <FormattedMessage id="credits.schedule" />,
        cell: (props) => (
          <FlexContainer alignItems="center">
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
        enableSorting: false,
      }),
      columnHelper.accessor("totalUsage", {
        header: () => <FormattedMessage id="credits.usage" />,
        meta: {
          responsive: true,
        },
        sortingFn: "basic",
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
  }, [columnHelper, workspaceId]);

  return (
    <div className={styles.content}>
      <Table
        variant="white"
        columns={columns}
        data={freeAndPaidUsageByConnection}
        initialSortBy={[{ id: "totalUsage", desc: true }]}
      />
    </div>
  );
};
