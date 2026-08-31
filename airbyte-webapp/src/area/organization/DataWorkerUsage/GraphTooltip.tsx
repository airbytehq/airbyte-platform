import dayjs from "dayjs";
import { FormattedDate, FormattedMessage, useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { Text } from "components/ui/Text";

import { RegionDataBar, UsageGraphGranularity } from "./calculateGraphData";
import styles from "./GraphTooltip.module.scss";

type ComparisonTimeRange = "1d" | "1w" | "1m" | "1q" | "1y";

export interface RegionComparisonDataBar {
  formattedDate: string;
  currentDate?: string;
  previousDate?: string;
  currentUsage: number | null;
  previousUsage: number | null;
}

interface WorkspaceMetadata {
  id: string;
  name: string;
}

interface BaseGraphTooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: RegionDataBar | RegionComparisonDataBar }>;
  barColor: string;
  granularity: UsageGraphGranularity;
}

interface SinglePeriodGraphTooltipProps {
  comparison?: undefined;
  regionName: string;
  top10Workspaces: WorkspaceMetadata[];
  hasOtherCategory: boolean;
}

interface ComparisonGraphTooltipProps {
  comparison: {
    comparisonBarColor: string;
    selectedTimeRange: ComparisonTimeRange;
  };
  regionName?: never;
  top10Workspaces?: never;
  hasOtherCategory?: never;
}

type GraphTooltipProps = BaseGraphTooltipProps & (SinglePeriodGraphTooltipProps | ComparisonGraphTooltipProps);

interface WorkspaceUsage {
  id: string;
  name: string;
  value: number;
}

const formatWorkerUsageNumber = (value: number) => value.toFixed(2);

const hasNonZeroUsage = ({ value }: WorkspaceUsage) => Number(formatWorkerUsageNumber(value)) > 0;

const sortByUsageDescendingThenByName = (a: WorkspaceUsage, b: WorkspaceUsage) => {
  const valueDiff = b.value - a.value;
  return valueDiff !== 0 ? valueDiff : a.name.localeCompare(b.name);
};

const COMPARISON_HEADING_ID_BY_RANGE: Record<ComparisonTimeRange, string> = {
  "1d": "dataWorkerUsage.comparison.heading.1d",
  "1w": "dataWorkerUsage.comparison.heading.1w",
  "1m": "dataWorkerUsage.comparison.heading.1m",
  "1q": "dataWorkerUsage.comparison.heading.1q",
  "1y": "dataWorkerUsage.comparison.heading.1y",
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

export const GraphTooltip = (props: GraphTooltipProps) => {
  const { active, payload, barColor, granularity } = props;
  const { formatDate, formatMessage } = useIntl();
  const graphData = payload?.[0]?.payload;

  if (!active || !graphData) {
    return null;
  }

  if (props.comparison) {
    if (!("currentUsage" in graphData)) {
      return null;
    }

    const formatComparisonDate = (date: string | undefined) => {
      if (!date) {
        return null;
      }

      const localDate = dayjs(date).toDate();
      return formatDate(
        localDate,
        granularity === "hour"
          ? {
              month: "short",
              day: "numeric",
              weekday: "short",
              hour: "numeric",
              minute: "2-digit",
              timeZoneName: "short",
            }
          : granularity === "week"
          ? { month: "short", day: "numeric", year: "numeric" }
          : { month: "short", day: "numeric", weekday: "short" }
      );
    };

    return (
      <Card noPadding className={styles.tooltip}>
        <div className={styles.content}>
          <Text as="div" size="lg" className={styles.comparisonHeading}>
            <FormattedMessage id={COMPARISON_HEADING_ID_BY_RANGE[props.comparison.selectedTimeRange]} />
          </Text>
          <div className={styles.details}>
            <Text as="div" color="grey" size="sm" className={styles.sectionLabel}>
              <FormattedMessage id="dataWorkerUsage.comparison.regionMax" />
            </Text>
            <ul className={styles.comparisonList}>
              <li className={styles.comparisonRow}>
                <div className={styles.regionIdentity}>
                  <span className={styles.regionSwatch} style={{ backgroundColor: barColor }} aria-hidden="true" />
                  <div className={styles.comparisonSeries}>
                    <Text as="span" size="lg">
                      <FormattedMessage id="dataWorkerUsage.comparison.current" />
                    </Text>
                    <Text as="span" color="grey" size="sm" className={styles.comparisonDate}>
                      {formatComparisonDate(graphData.currentDate)}
                    </Text>
                  </div>
                </div>
                <Text as="span" size="lg" className={styles.usageValue}>
                  {graphData.currentUsage === null ? (
                    <FormattedMessage id="dataWorkerUsage.comparison.unavailable" />
                  ) : (
                    <FormattedMessage
                      id="settings.organization.usage.graph.yAxisTick"
                      values={{ value: graphData.currentUsage.toFixed(2) }}
                    />
                  )}
                </Text>
              </li>
              <li className={styles.comparisonRow}>
                <div className={styles.regionIdentity}>
                  <span
                    className={styles.regionSwatch}
                    style={{ backgroundColor: props.comparison.comparisonBarColor }}
                    aria-hidden="true"
                  />
                  <div className={styles.comparisonSeries}>
                    <Text as="span" size="lg">
                      <FormattedMessage id="dataWorkerUsage.comparison.previous" />
                    </Text>
                    <Text as="span" color="grey" size="sm" className={styles.comparisonDate}>
                      {formatComparisonDate(graphData.previousDate)}
                    </Text>
                  </div>
                </div>
                <Text as="span" size="lg" className={styles.usageValue}>
                  {graphData.previousUsage === null ? (
                    <FormattedMessage id="dataWorkerUsage.comparison.unavailable" />
                  ) : (
                    <FormattedMessage
                      id="settings.organization.usage.graph.yAxisTick"
                      values={{ value: graphData.previousUsage.toFixed(2) }}
                    />
                  )}
                </Text>
              </li>
            </ul>
          </div>
        </div>
      </Card>
    );
  }

  if (!("workspaceUsage" in graphData)) {
    return null;
  }

  const { regionName, top10Workspaces, hasOtherCategory } = props;

  const workspacesSortedByUsage = top10Workspaces
    .map(({ id, name }) => ({ id, name, value: graphData.workspaceUsage[id] ?? 0 }))
    .filter(hasNonZeroUsage)
    .sort(sortByUsageDescendingThenByName);
  const otherWorkspaceUsage = graphData.workspaceUsage.other ?? 0;
  const showOtherWorkspaceUsage = hasOtherCategory && Number(formatWorkerUsageNumber(otherWorkspaceUsage)) > 0;
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
                  values={{ value: formatWorkerUsageNumber(graphData.maxWorkspaceUsage) }}
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
