import classNames from "classnames";
import React, { useMemo } from "react";
import { FormattedDate, FormattedMessage, FormattedTimeParts } from "react-intl";

import { JobProgress } from "components/connection/JobProgress";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { StatusIcon } from "components/ui/StatusIcon";
import { Text } from "components/ui/Text";

import { AttemptRead, JobStatus, SynchronousJobRead } from "core/request/AirbyteClient";

import { AttemptDetails } from "./AttemptDetails";
import styles from "./JobSummary.module.scss";
import { LinkToAttemptButton } from "./LinkToAttemptButton";
import { ResetStreamsDetails } from "./ResetStreamDetails";
import { JobWithAttempts } from "../types";
import { getJobId, getJobStatus } from "../utils";

const getJobConfig = (job: SynchronousJobRead | JobWithAttempts) =>
  (job as SynchronousJobRead).configType ?? (job as JobWithAttempts).job.configType;

export const getJobCreatedAt = (job: SynchronousJobRead | JobWithAttempts) =>
  (job as SynchronousJobRead).createdAt ?? (job as JobWithAttempts).job.createdAt;

const partialSuccessCheck = (attempts: AttemptRead[]) => {
  if (attempts.length > 0 && attempts[attempts.length - 1].status === JobStatus.failed) {
    return attempts.some((attempt) => attempt.failureSummary && attempt.failureSummary.partialSuccess);
  }
  return false;
};

interface JobSummaryProps {
  job: SynchronousJobRead | JobWithAttempts;
  attempts?: AttemptRead[];
  isOpen?: boolean;
  onExpand: () => void;
  isFailed?: boolean;
}

export const JobSummary: React.FC<JobSummaryProps> = ({ job, attempts = [], isOpen, onExpand, isFailed }) => {
  const jobStatus = getJobStatus(job);
  const jobConfigType = getJobConfig(job);
  const streamsToReset = "job" in job ? job.job.resetConfig?.streamsToReset : undefined;
  const isPartialSuccess = partialSuccessCheck(attempts);

  const statusIcon = useMemo(() => {
    if (!isPartialSuccess && isFailed) {
      return <StatusIcon status="error" />;
    } else if (jobStatus === JobStatus.cancelled) {
      return <StatusIcon status="cancelled" />;
    } else if (jobStatus === JobStatus.running) {
      return <StatusIcon status="loading" />;
    } else if (jobStatus === JobStatus.succeeded) {
      return <StatusIcon status="success" />;
    } else if (isPartialSuccess) {
      return <StatusIcon status="warning" />;
    }
    return null;
  }, [isFailed, isPartialSuccess, jobStatus]);

  const label = useMemo(() => {
    let status = "";
    if (jobStatus === JobStatus.failed) {
      status = "failed";
    } else if (jobStatus === JobStatus.cancelled) {
      status = "cancelled";
    } else if (jobStatus === JobStatus.running) {
      status = "running";
    } else if (jobStatus === JobStatus.succeeded) {
      status = "succeeded";
    } else if (isPartialSuccess) {
      status = "partialSuccess";
    } else {
      return <FormattedMessage id="jobs.jobStatus.unknown" />;
    }
    return (
      <FormattedMessage
        values={{ count: streamsToReset?.length || 0 }}
        id={`jobs.jobStatus.${jobConfigType}.${status}`}
      />
    );
  }, [isPartialSuccess, jobConfigType, jobStatus, streamsToReset?.length]);

  return (
    <FlexContainer className={classNames(styles.jobSummary, { [styles["jobSummary--open"]]: isOpen })}>
      <button className={classNames(styles.jobSummary__button, { [styles.failed]: isFailed })} onClick={onExpand}>
        <div className={styles.jobSummary__statusIcon}>{statusIcon}</div>
        <div className={styles.jobSummary__justification}>
          <Text size="md">{label}</Text>
          {jobConfigType === "sync" && <JobProgress job={job} expanded={isOpen} />}
          {attempts.length > 0 && (
            <>
              {jobConfigType === "reset_connection" ? (
                <ResetStreamsDetails isOpen={isOpen} names={streamsToReset?.map((stream) => stream.name)} />
              ) : (
                <AttemptDetails
                  jobId={getJobId(job)}
                  attempt={attempts[attempts.length - 1]}
                  hasMultipleAttempts={attempts.length > 1}
                />
              )}
            </>
          )}
        </div>
        <FlexContainer alignItems="center">
          <div className={styles.jobSummary__timestamp}>
            <Text>
              <FormattedTimeParts value={getJobCreatedAt(job) * 1000} hour="numeric" minute="2-digit">
                {(parts) => <span>{`${parts[0].value}:${parts[2].value}${parts[4].value} `}</span>}
              </FormattedTimeParts>
              <FormattedDate value={getJobCreatedAt(job) * 1000} month="2-digit" day="2-digit" year="numeric" />
            </Text>
            {attempts.length > 1 && (
              <Box mt="xs">
                <Text size="sm" color="grey" align="right">
                  <FormattedMessage id="sources.countAttempts" values={{ count: attempts.length }} />
                </Text>
              </Box>
            )}
          </div>
        </FlexContainer>
      </button>
      <FlexContainer alignItems="center">
        <Box pl="md">
          <LinkToAttemptButton jobId={String(getJobId(job))} attemptId={0} />
        </Box>
      </FlexContainer>
      <FlexContainer alignItems="center">
        {/* tabIndex can be -1 because the main button is still focusable. This one is just for convenience. */}
        <button onClick={onExpand} tabIndex={-1} className={styles.jobSummary__chevronButton}>
          <Icon type="chevronRight" size="xl" className={styles.jobSummary__chevron} color="action" />
        </button>
      </FlexContainer>
    </FlexContainer>
  );
};
