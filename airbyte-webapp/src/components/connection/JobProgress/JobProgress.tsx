import classNames from "classnames";
import { FormattedMessage, useIntl } from "react-intl";

import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { JobWithAttempts } from "area/connection/types/jobs";
import { getJobStatus } from "area/connection/utils/jobs";
import { AttemptRead, AttemptStatus, SynchronousJobRead } from "core/api/types/AirbyteClient";
import { formatBytes } from "core/utils/numberHelper";

import styles from "./JobProgress.module.scss";
import { ProgressLine } from "./JobProgressLine";
import { StreamProgress } from "./StreamProgress";
import { progressBarCalculations } from "./utils";

function isJobsWithJobs(job: JobWithAttempts | SynchronousJobRead): job is JobWithAttempts {
  return "attempts" in job;
}

interface ProgressBarProps {
  job: JobWithAttempts | SynchronousJobRead;
  expanded?: boolean;
}

export const JobProgress: React.FC<ProgressBarProps> = ({ job, expanded }) => {
  const { formatMessage, formatNumber } = useIntl();

  let latestAttempt: AttemptRead | undefined;
  if (isJobsWithJobs(job) && job.attempts) {
    latestAttempt = job.attempts[job.attempts.length - 1];
  }
  if (!latestAttempt) {
    return null;
  }

  const jobStatus = getJobStatus(job);
  if (["failed", "succeeded", "cancelled"].includes(jobStatus)) {
    return null;
  }

  const {
    displayProgressBar,
    totalPercentRecords,
    timeRemaining,
    numeratorBytes,
    numeratorRecords,
    denominatorRecords,
    denominatorBytes,
    elapsedTimeMS,
  } = progressBarCalculations(latestAttempt);

  let timeRemainingString = "";
  if (elapsedTimeMS && timeRemaining) {
    const minutesRemaining = Math.ceil(timeRemaining / 1000 / 60);
    const hoursRemaining = Math.ceil(minutesRemaining / 60);
    if (minutesRemaining <= 60) {
      timeRemainingString = formatMessage({ id: "connection.progress.minutesRemaining" }, { value: minutesRemaining });
    } else {
      timeRemainingString = formatMessage({ id: "connection.progress.hoursRemaining" }, { value: hoursRemaining });
    }
  }

  return (
    <Text as="div" size="md">
      {displayProgressBar && (
        <ProgressLine percent={totalPercentRecords} type={jobStatus === "incomplete" ? "warning" : "default"} />
      )}
      {latestAttempt?.status === AttemptStatus.running && (
        <>
          {displayProgressBar && (
            <div className={styles.estimationStats}>
              <span>{timeRemaining < Infinity && timeRemaining > 0 && timeRemainingString}</span>
              <span>{formatNumber(totalPercentRecords, { style: "percent", maximumFractionDigits: 0 })}</span>
            </div>
          )}
          {expanded && (
            <>
              {denominatorRecords > 0 && denominatorBytes > 0 && (
                <div className={styles.estimationDetails}>
                  <span>
                    <Icon type="table" className={styles.icon} />
                    <FormattedMessage
                      id="connection.progress.recordsSynced"
                      values={{
                        synced: numeratorRecords,
                        total: denominatorRecords,
                        speed: Math.round((numeratorRecords / elapsedTimeMS) * 1000),
                      }}
                    />
                  </span>
                  <span>
                    <Icon type="database" className={styles.icon} />
                    <FormattedMessage
                      id="connection.progress.bytesSynced"
                      values={{
                        synced: formatBytes(numeratorBytes),
                        total: formatBytes(denominatorBytes),
                        speed: formatBytes((numeratorBytes * 1000) / elapsedTimeMS),
                      }}
                    />
                  </span>
                </div>
              )}
              {latestAttempt.streamStats && (
                <div className={classNames(styles.streams)}>
                  {latestAttempt.streamStats
                    ?.map((stats) => ({
                      ...stats,
                      done: (stats.stats.recordsEmitted ?? 0) >= (stats.stats.estimatedRecords ?? Infinity),
                    }))
                    // Move finished streams to the end of the list
                    .sort((a, b) => Number(a.done) - Number(b.done))
                    .map((stream) => {
                      return <StreamProgress stream={stream} key={stream.streamName} />;
                    })}
                </div>
              )}
            </>
          )}
        </>
      )}
    </Text>
  );
};
