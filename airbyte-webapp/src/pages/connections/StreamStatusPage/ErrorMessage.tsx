import { useMemo } from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { FailureOrigin, FailureType, JobWithAttemptsRead } from "core/request/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./ErrorMessage.module.scss";

const getErrorMessageFromJob = (job: JobWithAttemptsRead | undefined) => {
  const latestAttempt = job?.attempts?.slice(-1)[0];
  if (latestAttempt?.failureSummary?.failures?.[0]?.failureType !== "manual_cancellation") {
    return {
      errorMessage: latestAttempt?.failureSummary?.failures?.[0]?.externalMessage,
      failureOrigin: latestAttempt?.failureSummary?.failures?.[0]?.failureOrigin,
      failureType: latestAttempt?.failureSummary?.failures?.[0]?.failureType,
      attemptId: latestAttempt?.id,
      jobId: job?.job?.id,
    };
  }

  return null;
};

export const ErrorMessage: React.FC = () => {
  const navigate = useNavigate();
  const { formatMessage } = useIntl();

  const workspaceId = useCurrentWorkspaceId();
  const { connection } = useConnectionEditService();
  const { lastCompletedSyncJob } = useConnectionSyncContext();
  const { hasSchemaChanges, hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);

  const calloutDetails = useMemo<{
    errorMessage: string;
    errorAction: () => void;
    buttonMessage: string;
    variant: "error" | "warning";
  } | null>(() => {
    const { jobId, attemptId, errorMessage, failureType, failureOrigin } =
      getErrorMessageFromJob(lastCompletedSyncJob) ?? {};
    // If we have an error message and no breaking schema changes, show the error message
    if (errorMessage && !hasBreakingSchemaChange) {
      const isConfigError = failureType === FailureType.config_error;
      const isSourceError = failureOrigin === FailureOrigin.source;
      const isDestinationError = failureOrigin === FailureOrigin.destination;

      if (isConfigError && (isSourceError || isDestinationError)) {
        const targetRoute = isSourceError ? RoutePaths.Source : RoutePaths.Destination;
        const targetRouteId = isSourceError ? connection.sourceId : connection.destinationId;
        return {
          errorMessage,
          errorAction: () => navigate(`/${RoutePaths.Workspaces}/${workspaceId}/${targetRoute}/${targetRouteId}`),
          buttonMessage: formatMessage({ id: "connection.stream.status.gotoSettings" }),
          variant: "warning",
        };
      }

      return {
        errorMessage,
        errorAction: () => navigate(`../${ConnectionRoutePaths.JobHistory}#${jobId}::${attemptId}`),
        buttonMessage: formatMessage({ id: "connection.stream.status.seeLogs" }),
        variant: "warning",
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
        variant: hasBreakingSchemaChange ? "error" : "warning",
      };
    }

    return null;
  }, [
    formatMessage,
    hasBreakingSchemaChange,
    hasSchemaChanges,
    lastCompletedSyncJob,
    navigate,
    connection.sourceId,
    connection.destinationId,
    workspaceId,
  ]);

  if (calloutDetails) {
    return (
      <Box p="lg">
        <FlexContainer>
          <Message
            text={calloutDetails.errorMessage}
            actionBtnText={calloutDetails.buttonMessage}
            type={calloutDetails.variant}
            onAction={calloutDetails.errorAction}
            className={styles.error}
          />
        </FlexContainer>
      </Box>
    );
  }

  return null;
};
