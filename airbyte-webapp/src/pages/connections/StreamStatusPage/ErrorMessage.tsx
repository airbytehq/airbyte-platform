import classNames from "classnames";
import { useMemo } from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { AirbyteStreamWithStatusAndConfiguration } from "components/connection/StreamStatus/getStreamsWithStatus";
import { StreamStatusType, useGetStreamStatus } from "components/connection/StreamStatus/streamStatusUtils";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";

import { JobStatus, JobWithAttemptsRead } from "core/request/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

import styles from "./ErrorMessage.module.scss";
import { useStreamsListContext } from "./StreamsListContext";
import { ConnectionRoutePaths } from "../types";

const getErrorMessageFromJob = (job: JobWithAttemptsRead | undefined) => {
  const latestAttempt = job?.attempts?.slice(-1)[0];
  if (latestAttempt?.failureSummary?.failures?.[0]?.failureType !== "manual_cancellation") {
    return {
      errorMessage: latestAttempt?.failureSummary?.failures?.[0]?.externalMessage,
      attemptId: latestAttempt?.id,
      jobId: job?.job?.id,
    };
  }

  return null;
};

export const ErrorMessage: React.FC = () => {
  const navigate = useNavigate();
  const { formatMessage } = useIntl();

  const { jobs } = useStreamsListContext();

  const { connection } = useConnectionEditService();
  const { hasSchemaChanges, hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);

  const calloutDetails = useMemo<{
    errorMessage: string;
    errorAction: () => void;
    buttonMessage: string;
    variant: "error" | "info";
  } | null>(() => {
    const { jobId, attemptId, errorMessage } = getErrorMessageFromJob(jobs?.[0]) ?? {};
    // If we have an error message and a non-breaking schema change, show the error message
    if (errorMessage && !hasBreakingSchemaChange) {
      return {
        errorMessage,
        errorAction: () => navigate(`../${ConnectionRoutePaths.JobHistory}#${jobId}::${attemptId}`),
        buttonMessage: formatMessage({ id: "connection.stream.status.seeLogs" }),
        variant: "error",
      };
    }

    // If we have schema changes, show the correct message
    if (hasSchemaChanges) {
      return {
        errorMessage: formatMessage({
          id: `connection.schemaChange.${hasBreakingSchemaChange ? "breaking" : "nonBreaking"}`,
        }),
        errorAction: () =>
          navigate(`../${ConnectionRoutePaths.Replication}`, { state: { triggerRefreshSchema: true } }),
        buttonMessage: formatMessage({ id: "connection.schemaChange.reviewAction" }),
        variant: "info",
      };
    }

    return null;
  }, [formatMessage, hasBreakingSchemaChange, hasSchemaChanges, jobs, navigate]);

  if (calloutDetails) {
    return (
      <FlexContainer>
        <Message
          text={calloutDetails.errorMessage}
          actionBtnText={calloutDetails.buttonMessage}
          type={calloutDetails.variant}
          onAction={calloutDetails.errorAction}
          className={styles.error}
        />
      </FlexContainer>
    );
  }

  return null;
};

export const StreamErrorMessage: React.FC<{ stream: AirbyteStreamWithStatusAndConfiguration }> = ({ stream }) => {
  const navigate = useNavigate();
  const { formatMessage } = useIntl();

  const { jobs } = useStreamsListContext();

  const { activeJob } = useConnectionSyncContext();
  const streamJob = jobs.find((job) => job.job?.id && stream.config?.jobId && job.job.id === stream.config.jobId);
  const streamStatus = useGetStreamStatus()(stream.config);
  const jobErrorMessage = getErrorMessageFromJob(streamJob)?.errorMessage;

  const errorMessage =
    jobErrorMessage ??
    formatMessage(
      { id: "connection.stream.status.genericError" },
      { syncType: formatMessage({ id: `sources.${streamJob?.job?.configType ?? "sync"}` }).toLowerCase() }
    );

  if (activeJob?.status === JobStatus.running || streamStatus === StreamStatusType.UpToDate) {
    return null;
  }

  const message = (
    <Message
      text={errorMessage}
      actionBtnText={formatMessage({ id: "connection.stream.status.seeLogs" })}
      type="error"
      onAction={() =>
        navigate(`../${ConnectionRoutePaths.JobHistory}#${stream.config?.jobId}::${stream.config?.attemptId}`)
      }
      className={classNames(styles.error)}
      hideIcon
    />
  );

  return <Box ml="2xl">{message}</Box>;
};
