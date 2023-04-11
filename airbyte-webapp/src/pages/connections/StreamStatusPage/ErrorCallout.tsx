import { useMemo } from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";

import { JobWithAttemptsRead } from "core/request/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

import styles from "./ErrorCallout.module.scss";
import { useStreamsListContext } from "./StreamsListContext";
import { ConnectionRoutePaths } from "../types";

const getLatestErrorMessage = (job: JobWithAttemptsRead | undefined) => {
  const latestAttempt = job?.attempts?.slice(-1)[0];
  if (latestAttempt?.failureSummary?.failures?.[0]?.failureType !== "manual_cancellation") {
    return {
      errorMessage: latestAttempt?.failureSummary?.failures?.[0]?.externalMessage,
      attemptId: latestAttempt?.id,
      jobId: job?.job?.id,
    };
  }

  return {};
};

export const ErrorCallout = () => {
  const { connection } = useConnectionEditService();
  const navigate = useNavigate();
  const { hasSchemaChanges, hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);

  const { jobs } = useStreamsListContext();

  const { formatMessage } = useIntl();

  const calloutDetails = useMemo<{
    errorMessage: string;
    errorAction: () => void;
    buttonMessage: string;
    variant: "error" | "info";
  } | null>(() => {
    const { jobId, attemptId, errorMessage } = getLatestErrorMessage(jobs?.[0]);
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
