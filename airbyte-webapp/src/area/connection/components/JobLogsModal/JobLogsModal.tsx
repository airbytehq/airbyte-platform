import { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Message } from "components/ui/Message";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import { AttemptDetails } from "area/connection/components/AttemptDetails";
import { LinkToAttemptButton } from "area/connection/components/JobLogsModal/LinkToAttemptButton";
import { useAttemptForJob, useDonwnloadJobLogsFetchQuery, useJobInfoWithoutLogs } from "core/api";
import { WebBackendConnectionRead } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";

import { AttemptLogs } from "./AttemptLogs";
import { AttemptStatusIcon } from "./AttemptStatusIcon";
import { DownloadLogsButton } from "./DownloadLogsButton";
import styles from "./JobLogsModal.module.scss";

interface JobLogsModalProps {
  connection: WebBackendConnectionRead;
  jobId: number;
  initialAttemptId?: number;
  eventId?: string;
}

export const JobLogsModal: React.FC<JobLogsModalProps> = ({ jobId, initialAttemptId, eventId, connection }) => {
  const job = useJobInfoWithoutLogs(jobId);

  if (job.attempts.length === 0) {
    trackError(new Error(`No attempts for job`), { jobId, eventId });

    return (
      <Box p="lg">
        <Message type="warning" text={<FormattedMessage id="jobHistory.logs.noAttempts" />} />
      </Box>
    );
  }

  return (
    <JobLogsModalInner jobId={jobId} initialAttemptId={initialAttemptId} eventId={eventId} connection={connection} />
  );
};

const JobLogsModalInner: React.FC<JobLogsModalProps> = ({ jobId, initialAttemptId, eventId, connection }) => {
  const job = useJobInfoWithoutLogs(jobId);

  const [selectedAttemptId, setSelectedAttemptId] = useState(
    initialAttemptId ?? job.attempts[job.attempts.length - 1].attempt.id
  );

  const { data: jobAttempt } = useAttemptForJob(jobId, selectedAttemptId);

  const downloadLogs = useDonwnloadJobLogsFetchQuery();

  const { formatMessage } = useIntl();

  const attemptListboxOptions = useMemo(() => {
    return job.attempts.map((attempt, index) => ({
      label: formatMessage(
        { id: "jobHistory.logs.attemptLabel" },
        { attemptNumber: index + 1, totalAttempts: job.attempts.length }
      ),
      value: attempt.attempt.id,
      icon: <AttemptStatusIcon attempt={attempt} />,
    }));
  }, [job, formatMessage]);

  return (
    <FlexContainer direction="column" className={styles.jobLogsContainer} data-testid="job-logs-modal">
      <Box p="md" pb="none">
        <FlexContainer alignItems="center">
          <div className={styles.attemptDropdown}>
            <ListBox
              buttonClassName={styles.attemptDropdown__listbox}
              selectedValue={selectedAttemptId}
              options={attemptListboxOptions}
              onSelect={setSelectedAttemptId}
              isDisabled={job.attempts.length === 1}
            />
          </div>
          {jobAttempt ? (
            <AttemptDetails attempt={jobAttempt.attempt} jobId={jobId} showEndedAt showFailureMessage={false} />
          ) : (
            <FlexContainer direction="column">
              <LoadingSkeleton />
            </FlexContainer>
          )}
          <FlexContainer className={styles.downloadLogs}>
            <LinkToAttemptButton
              connectionId={connection.connectionId}
              jobId={jobId}
              attemptId={selectedAttemptId}
              eventId={eventId}
            />
            <DownloadLogsButton downloadLogs={() => downloadLogs(connection.name, jobId)} />
          </FlexContainer>
        </FlexContainer>
      </Box>
      {jobAttempt && <AttemptLogs attempt={jobAttempt} />}
      {!jobAttempt && (
        <div className={styles.attemptLoading}>
          <Spinner />
          <Text>
            <FormattedMessage id="jobHistory.logs.loadingAttempt" />
          </Text>
        </div>
      )}
    </FlexContainer>
  );
};
