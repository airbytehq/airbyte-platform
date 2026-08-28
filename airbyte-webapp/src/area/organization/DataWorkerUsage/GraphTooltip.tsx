import dayjs from "dayjs";
import { FormattedDate, FormattedMessage, useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { Text } from "components/ui/Text";

import { RegionDataBar, UsageGraphGranularity } from "./calculateGraphData";
import styles from "./GraphTooltip.module.scss";

interface WorkspaceMetadata {
  id: string;
  name: string;
}

interface GraphTooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: RegionDataBar }>;
  regionName: string;
  top10Workspaces: WorkspaceMetadata[];
  hasOtherCategory: boolean;
  barColor: string;
  granularity: UsageGraphGranularity;
}

interface WorkspaceUsage {
  id: string;
  name: string;
  value: number;
}

const formatWorkerUsageNumber = (value: number) => Number(value.toFixed(1));

const hasNonZeroUsage = ({ value }: WorkspaceUsage) => formatWorkerUsageNumber(value) > 0;

const sortByUsageDescendingThenByName = (a: WorkspaceUsage, b: WorkspaceUsage) => {
  const valueDiff = b.value - a.value;
  return valueDiff !== 0 ? valueDiff : a.name.localeCompare(b.name);
};

const WorkspaceUsageRow = ({ name, value }: Pick<WorkspaceUsage, "name" | "value">) => (
  <li className={styles.workspaceRow}>
    <Text as="span" color="grey" size="lg" className={styles.workspaceName}>
      {name}
    </Text>
    <Text as="span" size="lg" className={styles.usageValue}>
      {formatWorkerUsageNumber(value)}
    </Text>
  </li>
);

export const GraphTooltip = ({
  active,
  payload,
  regionName,
  top10Workspaces,
  hasOtherCategory,
  barColor,
  granularity,
}: GraphTooltipProps) => {
  const { formatMessage } = useIntl();
  const graphData = payload?.[0]?.payload;

  if (!active || !graphData) {
    return null;
  }

  const workspacesSortedByUsage = top10Workspaces
    .map(({ id, name }) => ({ id, name, value: graphData.workspaceUsage[id] ?? 0 }))
    .filter(hasNonZeroUsage)
    .sort(sortByUsageDescendingThenByName);
  const otherWorkspaceUsage = graphData.workspaceUsage.other ?? 0;
  const showOtherWorkspaceUsage = hasOtherCategory && formatWorkerUsageNumber(otherWorkspaceUsage) > 0;
  const localDate = dayjs(graphData.formattedDate).toDate();

  return (
    <Card noPadding className={styles.tooltip}>
      <div className={styles.content}>
        <Text as="div" size="lg" className={styles.date}>
          {granularity === "hour" ? (
            <FormattedDate
              value={localDate}
              month="short"
              day="numeric"
              weekday="short"
              hour="numeric"
              minute="2-digit"
            />
          ) : granularity === "week" ? (
            <FormattedDate value={localDate} month="short" day="numeric" year="numeric" />
          ) : (
            <FormattedDate value={localDate} month="short" day="numeric" weekday="short" />
          )}
        </Text>

        <div className={styles.details}>
          <div className={styles.regionSection}>
            <Text as="div" color="grey" size="sm" className={styles.sectionLabel}>
              <FormattedMessage id="settings.organization.usage.graph.tooltip.regionMax" />
            </Text>
            <div className={styles.regionRow}>
              <div className={styles.regionIdentity}>
                <span className={styles.regionSwatch} style={{ backgroundColor: barColor }} aria-hidden="true" />
                <Text as="span" size="lg" className={styles.workspaceName}>
                  {regionName}
                </Text>
              </div>
              <Text as="span" size="lg" className={styles.usageValue}>
                <FormattedMessage
                  id="settings.organization.usage.graph.yAxisTick"
                  values={{ value: formatWorkerUsageNumber(graphData.regionUsage) }}
                />
              </Text>
            </div>
          </div>

          <div className={styles.workspaceSection}>
            <div className={styles.workspaceHeader}>
              <Text as="span" color="grey" size="sm" className={styles.sectionLabel}>
                <FormattedMessage id="settings.organization.usage.graph.tooltip.workspaceUsage" />
              </Text>
              <Text as="span" color="grey" size="sm" className={styles.sectionMetadata}>
                <FormattedMessage id="settings.organization.usage.graph.tooltip.top10" />
              </Text>
            </div>
            <Text color="grey" size="sm" className={styles.workspaceNote}>
              <FormattedMessage id="settings.organization.usage.graph.tooltip.workspaceUsageDescription" />
            </Text>
            <ul
              className={styles.workspaceList}
              aria-label={formatMessage({ id: "settings.organization.usage.graph.tooltip.workspaceUsage" })}
            >
              {workspacesSortedByUsage.map(({ id, name, value }) => (
                <WorkspaceUsageRow key={id} name={name} value={value} />
              ))}
              {showOtherWorkspaceUsage && (
                <WorkspaceUsageRow
                  name={formatMessage({ id: "settings.organization.usage.graph.tooltip.other" })}
                  value={otherWorkspaceUsage}
                />
              )}
            </ul>
          </div>
        </div>
      </div>
    </Card>
  );
};
