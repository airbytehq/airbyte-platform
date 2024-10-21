import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Table } from "components/ui/Table";
import { TextWithOverflowTooltip } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { FormattedCredits } from "packages/cloud/area/billing/components/FormattedCredits";
import { UsagePerDayGraph } from "packages/cloud/area/billing/components/UsagePerDayGraph";
import { RoutePaths } from "pages/routePaths";

import { useOrganizationCreditsContext, WorkspaceUsage } from "./OrganizationCreditContext";
import styles from "./UsageByWorkspaceTable.module.scss";
import { CloudSettingsRoutePaths } from "../../settings/routePaths";

export const UsageByWorkspaceTable: React.FC = () => {
  const { freeAndPaidUsageByWorkspace } = useOrganizationCreditsContext();
  const columnHelper = useMemo(() => createColumnHelper<WorkspaceUsage>(), []);

  const columns = React.useMemo(() => {
    return [
      columnHelper.accessor("workspace.name", {
        header: () => <FormattedMessage id="credits.workspace" />,
        meta: {
          responsive: true,
        },
        sortingFn: "alphanumeric",
        cell: (props) => {
          return props.row.original.workspace.tombstoned ? (
            <TextWithOverflowTooltip size="sm" color="grey300" className={classNames(styles.cellText)}>
              <FlexContainer alignItems="center" gap="sm">
                {props.cell.getValue()}
                <Tooltip control={<Icon type="trash" size="sm" />}>
                  <FormattedMessage id="credits.workspace.deleted" />
                </Tooltip>
              </FlexContainer>
            </TextWithOverflowTooltip>
          ) : (
            <Link
              to={`/${RoutePaths.Workspaces}/${props.row.original.workspace.id}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Usage}`}
            >
              <TextWithOverflowTooltip size="sm" className={classNames(styles.cellText)}>
                {props.cell.getValue()}
              </TextWithOverflowTooltip>
            </Link>
          );
        },
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
              [styles.deleted]: props.row.original.workspace.tombstoned,
            })}
          >
            <UsagePerDayGraph chartData={props.row.original.usage} minimized />
            <FlexContainer direction="column" gap="none" className={styles.usageTotals}>
              {props.row.original.totalFreeUsage > 0 && (
                <FormattedCredits
                  credits={props.row.original.totalFreeUsage}
                  color={props.row.original.workspace.tombstoned ? "grey300" : "green"}
                  size="sm"
                />
              )}
              {props.row.original.totalInternalUsage > 0 && (
                <FormattedCredits
                  credits={props.row.original.totalInternalUsage}
                  color={props.row.original.workspace.tombstoned ? "grey300" : "blue"}
                  size="sm"
                />
              )}
              {props.row.original.totalBilledCost > 0 && (
                <FormattedCredits
                  color={props.row.original.workspace.tombstoned ? "grey300" : undefined}
                  credits={props.row.original.totalBilledCost}
                  size="sm"
                />
              )}
            </FlexContainer>
          </FlexContainer>
        ),
      }),
    ];
  }, [columnHelper]);

  return (
    <div className={styles.content}>
      <Table
        rowId={(row) => row.workspace.id}
        variant="white"
        columns={columns}
        data={freeAndPaidUsageByWorkspace}
        initialSortBy={[{ id: "totalUsage", desc: true }]}
      />
    </div>
  );
};
