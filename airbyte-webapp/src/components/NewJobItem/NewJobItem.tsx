import { faEllipsisV } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import { Suspense, useMemo, useRef } from "react";
import { FormattedDate, FormattedMessage, FormattedTimeParts, useIntl } from "react-intl";
import { useLocation, useNavigate } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { buildAttemptLink, useAttemptLink } from "components/JobItem/attemptLinkUtils";
import { AttemptDetails } from "components/JobItem/components/AttemptDetails";
import { getJobCreatedAt } from "components/JobItem/components/JobSummary";
import { JobWithAttempts } from "components/JobItem/types";
import { getJobAttempts, getJobStatus } from "components/JobItem/utils";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import { AttemptRead, JobStatus } from "core/request/AirbyteClient";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";
import { copyToClipboard } from "utils/clipboard";

import { JobLogsModalContent } from "./JobLogsModalContent";
import { JobStatusIcon } from "./JobStatusIcon";
import styles from "./NewJobItem.module.scss";

export const partialSuccessCheck = (attempts: AttemptRead[]) => {
  if (attempts.length > 0 && attempts[attempts.length - 1].status === JobStatus.failed) {
    return attempts.some((attempt) => attempt.failureSummary && attempt.failureSummary.partialSuccess);
  }
  return false;
};

interface NewJobItemProps {
  jobWithAttempts: JobWithAttempts;
}

enum ContextMenuOptions {
  OpenLogsModal = "OpenLogsModal",
  CopyLinkToJob = "CopyLinkToJob",
}

export const NewJobItem: React.FC<NewJobItemProps> = ({ jobWithAttempts }) => {
  const { openModal } = useModalService();
  const jobStatus = getJobStatus(jobWithAttempts);
  const attempts = getJobAttempts(jobWithAttempts);
  const isPartialSuccess = attempts && partialSuccessCheck(attempts);
  const streamsToReset = "job" in jobWithAttempts ? jobWithAttempts.job.resetConfig?.streamsToReset : undefined;
  const jobConfigType = jobWithAttempts.job.configType;
  const attemptLink = useAttemptLink();
  const wrapperRef = useRef<HTMLDivElement>(null);
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const location = useLocation();
  const navigate = useNavigate();

  useEffectOnce(() => {
    if (attemptLink.jobId === String(jobWithAttempts.job.id)) {
      wrapperRef.current?.scrollIntoView();
    }
  });

  const handleClick = (optionClicked: DropdownMenuOptionType) => {
    switch (optionClicked.value) {
      case ContextMenuOptions.OpenLogsModal:
        if (attemptLink.jobId) {
          // Clear the hash to remove the highlighted job from the UI
          navigate(location.pathname);
        }
        openModal({
          size: "full",
          title: formatMessage({ id: "jobHistory.logs.title" }, { jobId: jobWithAttempts.job.id }),
          content: () => (
            <Suspense
              fallback={
                <div className={styles.newJobItem__modalLoading}>
                  <Spinner />
                </div>
              }
            >
              <JobLogsModalContent jobId={jobWithAttempts.job.id} job={jobWithAttempts} />
            </Suspense>
          ),
        });
        break;
      case ContextMenuOptions.CopyLinkToJob:
        const url = new URL(window.location.href);
        url.hash = buildAttemptLink(
          jobWithAttempts.job.id,
          jobWithAttempts.attempts[jobWithAttempts.attempts.length - 1].id
        );
        copyToClipboard(url.href);
        registerNotification({
          type: "success",
          text: formatMessage({ id: "jobHistory.copyLinkToJob.success" }),
          id: "jobHistory.copyLinkToJob.success",
        });
        break;
    }
  };

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
    <div
      ref={wrapperRef}
      className={classNames(styles.newJobItem, {
        [styles["newJobItem--linked"]]: attemptLink.jobId === String(jobWithAttempts.job.id),
      })}
      id={String(jobWithAttempts.job.id)}
    >
      <Box pr="xl">
        <JobStatusIcon job={jobWithAttempts} />
      </Box>
      <FlexItem grow>
        <FlexContainer justifyContent="space-between" alignItems="center">
          <Box>
            <Text>{label}</Text>
            {attempts && attempts.length > 0 && (
              <AttemptDetails
                attempt={attempts[attempts.length - 1]}
                hasMultipleAttempts={attempts.length > 1}
                jobId={String(jobWithAttempts.job.id)}
              />
            )}
          </Box>
          <Box pr="lg" className={styles.newJobItem__timestamp}>
            <Text>
              <FormattedTimeParts value={getJobCreatedAt(jobWithAttempts) * 1000} hour="numeric" minute="2-digit">
                {(parts) => <span>{`${parts[0].value}:${parts[2].value}${parts[4].value} `}</span>}
              </FormattedTimeParts>
              <FormattedDate
                value={getJobCreatedAt(jobWithAttempts) * 1000}
                month="2-digit"
                day="2-digit"
                year="numeric"
              />
            </Text>
            {jobWithAttempts.attempts.length > 1 && (
              <Box mt="xs">
                <Text size="sm" color="grey" align="right">
                  <FormattedMessage id="sources.countAttempts" values={{ count: jobWithAttempts.attempts.length }} />
                </Text>
              </Box>
            )}
          </Box>
        </FlexContainer>
      </FlexItem>
      <DropdownMenu
        placement="bottom-end"
        options={[
          { displayName: formatMessage({ id: "jobHistory.copyLinkToJob" }), value: ContextMenuOptions.CopyLinkToJob },
          { displayName: formatMessage({ id: "jobHistory.viewLogs" }), value: ContextMenuOptions.OpenLogsModal },
        ]}
        onChange={handleClick}
      >
        {() => <Button variant="clear" icon={<FontAwesomeIcon icon={faEllipsisV} />} />}
      </DropdownMenu>
    </div>
  );
};
