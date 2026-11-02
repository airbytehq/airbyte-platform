import classNames from "classnames";
import React from "react";
import { FormattedMessage, FormattedTime, useIntl } from "react-intl";

import { FormattedTimeRange } from "components/ui/FormattedTimeRange";
import { Tooltip } from "components/ui/Tooltip";

import { DataplaneHealthHistoryBucket } from "core/api/types/AirbyteClient";
import { useCurrentTime } from "core/utils/time";

import styles from "./HealthHistoryBar.module.scss";
import { healthStatusMessageId } from "./healthStatus";

export const HISTORY_PERIOD_MS = 15 * 60 * 1000;

interface HealthHistoryBarProps {
  history?: DataplaneHealthHistoryBucket[];
}

const periodCountMessageId: Record<DataplaneHealthHistoryBucket["status"], string> = {
  HEALTHY: "settings.organization.regions.dataplane.history.periods.healthy",
  DEGRADED: "settings.organization.regions.dataplane.history.periods.degraded",
  UNHEALTHY: "settings.organization.regions.dataplane.history.periods.unhealthy",
  UNKNOWN: "settings.organization.regions.dataplane.history.periods.unknown",
};

const PERIOD_STATUSES = Object.keys(periodCountMessageId) as Array<DataplaneHealthHistoryBucket["status"]>;

const segmentStyleByStatus: Record<DataplaneHealthHistoryBucket["status"], string> = {
  HEALTHY: styles.healthy,
  DEGRADED: styles.degraded,
  UNHEALTHY: styles.unhealthy,
  UNKNOWN: styles.unknown,
};

// The last period is the current clock quarter, still running, so its tooltip reads "since" instead of a closed range.
const PeriodLabel: React.FC<{ period: DataplaneHealthHistoryBucket; now: number }> = ({ period, now }) => {
  const start = new Date(period.bucket_start);
  const end = new Date(period.bucket_start + HISTORY_PERIOD_MS);
  const status = <FormattedMessage id={healthStatusMessageId[period.status]} />;

  if (end.getTime() > now) {
    return (
      <FormattedMessage
        id="settings.organization.regions.dataplane.history.currentSegment"
        values={{ start: <FormattedTime value={start} hour="numeric" minute="numeric" />, status }}
      />
    );
  }
  return (
    <FormattedMessage
      id="settings.organization.regions.dataplane.history.segment"
      values={{ range: <FormattedTimeRange from={start} to={end} />, status }}
    />
  );
};

// The segments cannot take focus and their tooltips need a pointer, so the bar carries a text summary for everyone else.
const useHistorySummary = (history: DataplaneHealthHistoryBucket[]): string => {
  const { formatMessage, formatList } = useIntl();
  const counts = PERIOD_STATUSES.map((status) => ({
    status,
    count: history.filter((period) => period.status === status).length,
  })).filter(({ count }) => count > 0);
  const countsText = formatList(
    counts.map(({ status, count }) => formatMessage({ id: periodCountMessageId[status] }, { count })),
    { type: "unit" }
  );
  return formatMessage({ id: "settings.organization.regions.dataplane.history.summary" }, { counts: countsText });
};

export const HealthHistoryBar: React.FC<HealthHistoryBarProps> = ({ history = [] }) => {
  const { formatMessage } = useIntl();
  const now = useCurrentTime();
  const summary = useHistorySummary(history);

  if (history.length === 0) {
    return (
      <div
        className={styles.bar}
        role="img"
        aria-label={formatMessage({ id: "settings.organization.regions.dataplane.history.empty" })}
        data-testid="health-history-bar"
      >
        <div className={classNames(styles.segment, styles.unknown)} data-testid="health-history-empty" />
      </div>
    );
  }

  return (
    <div className={styles.bar} role="img" aria-label={summary} data-testid="health-history-bar">
      {history.map((period) => (
        <Tooltip
          key={period.bucket_start}
          containerClassName={styles.segmentContainer}
          control={
            <div
              className={classNames(styles.segment, segmentStyleByStatus[period.status])}
              data-testid="health-history-segment"
              data-status={period.status}
            />
          }
        >
          <PeriodLabel period={period} now={now} />
        </Tooltip>
      ))}
    </div>
  );
};
