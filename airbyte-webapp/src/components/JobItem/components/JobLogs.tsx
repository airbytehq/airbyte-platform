import clamp from "lodash/clamp";
import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation } from "react-router-dom";

import Logs from "components/Logs";
import { Box } from "components/ui/Box";
import { StatusIcon } from "components/ui/StatusIcon";
import { StatusIconStatus } from "components/ui/StatusIcon/StatusIcon";
import { StepsMenu } from "components/ui/StepsMenu";
import { StepMenuItem } from "components/ui/StepsMenu/StepsMenu";
import { Text } from "components/ui/Text";

import { useGetDebugInfoJob } from "core/api";
import { AttemptRead, AttemptStatus, SynchronousJobRead } from "core/request/AirbyteClient";

import styles from "./JobLogs.module.scss";
import { LogsDetails } from "./LogsDetails";
import { parseAttemptLink } from "../attemptLinkUtils";
import { JobWithAttempts } from "../types";
import { getJobId, isCancelledAttempt } from "../utils";

interface JobLogsProps {
  jobIsFailed?: boolean;
  job: SynchronousJobRead | JobWithAttempts;
}

const mapAttemptStatusToIcon = (attempt: AttemptRead): StatusIconStatus => {
  if (isPartialSuccess(attempt)) {
    return "warning";
  }

  if (isCancelledAttempt(attempt)) {
    return "cancelled";
  }

  switch (attempt.status) {
    case AttemptStatus.running:
      return "loading";
    case AttemptStatus.succeeded:
      return "success";
    case AttemptStatus.failed:
      return "error";
  }
};

const isPartialSuccess = (attempt: AttemptRead) => {
  return !!attempt.failureSummary?.partialSuccess;
};

const jobIsSynchronousJobRead = (job: SynchronousJobRead | JobWithAttempts): job is SynchronousJobRead => {
  return !!(job as SynchronousJobRead)?.logs?.logLines;
};

export const JobLogs: React.FC<JobLogsProps> = ({ job }) => {
  const isSynchronousJobRead = jobIsSynchronousJobRead(job);

  const id: number | string = (job as JobWithAttempts).job?.id ?? (job as SynchronousJobRead).id;

  const debugInfo = useGetDebugInfoJob(id, typeof id === "number", true);

  const { hash } = useLocation();
  const [attemptNumber, setAttemptNumber] = useState<number>(() => {
    // If the link lead directly to an attempt use this attempt as the starting one
    // otherwise use the latest attempt
    if (!isSynchronousJobRead && job.attempts) {
      const { attemptId, jobId } = parseAttemptLink(hash);
      if (!isNaN(Number(jobId)) && Number(jobId) === job.job.id && attemptId) {
        return clamp(parseInt(attemptId), 0, job.attempts.length - 1);
      }

      return job.attempts.length ? job.attempts.length - 1 : 0;
    }

    return 0;
  });

  if (isSynchronousJobRead) {
    return <Logs logsArray={debugInfo?.attempts[attemptNumber]?.logs.logLines ?? job.logs?.logLines} />;
  }

  const currentAttempt = job.attempts?.[attemptNumber];
  const path = ["/tmp/workspace", job.job.id, currentAttempt?.id, "logs.log"].join("/");

  const attemptsTabs: StepMenuItem[] =
    job.attempts?.map((item, index) => ({
      id: index.toString(),
      icon: (
        <Box mr="md">
          <StatusIcon status={mapAttemptStatusToIcon(item)} />
        </Box>
      ),
      name: <FormattedMessage id="sources.attemptNum" values={{ number: index + 1 }} />,
    })) ?? [];

  const attempts = job.attempts?.length ?? 0;

  return (
    <>
      {attempts > 1 && (
        <StepsMenu
          lightMode
          activeStep={attemptNumber.toString()}
          onSelect={(at) => setAttemptNumber(parseInt(at))}
          data={attemptsTabs}
        />
      )}
      {attempts ? (
        <LogsDetails
          jobId={getJobId(job)}
          path={path}
          currentAttempt={currentAttempt}
          jobDebugInfo={debugInfo}
          showAttemptStats={attempts > 1}
          logs={debugInfo?.attempts[attemptNumber]?.logs.logLines}
        />
      ) : (
        <Text size="md" className={styles.jobStartFailure}>
          <FormattedMessage id="jobs.noAttemptsFailure" />
        </Text>
      )}
    </>
  );
};

export default JobLogs;
