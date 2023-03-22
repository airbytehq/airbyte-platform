import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { Callout } from "components/ui/Callout";
import { CalloutVariant } from "components/ui/Callout/Callout";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { JobWithAttemptsRead } from "core/request/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useListJobs } from "services/job/JobService";

import styles from "./ErrorCallout.module.scss";
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

  const { jobs } = useListJobs({
    configId: connection.connectionId,
    configTypes: ["sync", "reset_connection"],
    pagination: {
      pageSize: 1,
    },
  });

  const { formatMessage } = useIntl();

  const calloutDetails = useMemo<{
    errorMessage: string;
    errorAction: () => void;
    buttonMessage: string;
    variant: CalloutVariant;
  } | null>(() => {
    const { jobId, attemptId, errorMessage } = getLatestErrorMessage(jobs?.[0]);
    // If we have an error message and a non-breaking schema change, show the error message
    if (errorMessage && !hasBreakingSchemaChange) {
      return {
        errorMessage,
        errorAction: () => navigate(`../${ConnectionRoutePaths.JobHistory}#${jobId}::${attemptId}`),
        buttonMessage: "connection.stream.status.seeLogs",
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
        buttonMessage: "connection.schemaChange.reviewAction",
        variant: "actionRequired",
      };
    }

    return null;
  }, [formatMessage, hasBreakingSchemaChange, hasSchemaChanges, jobs, navigate]);

  if (calloutDetails) {
    return (
      <FlexContainer className={styles.callout}>
        <Callout variant={calloutDetails.variant} className={styles.error}>
          <Text className={styles.message}>{calloutDetails.errorMessage}</Text>
          <Button variant="dark" data-testid="calloutErrorButton" onClick={calloutDetails.errorAction}>
            <FormattedMessage id={calloutDetails.buttonMessage} />
          </Button>
        </Callout>
      </FlexContainer>
    );
  }
  return null;
};
