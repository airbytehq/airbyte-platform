import { useMemo, useState } from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Message, MessageProps, MessageType, isHigherSeverity, MESSAGE_SEVERITY_LEVELS } from "components/ui/Message";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useDestinationDefinitionVersion, useSourceDefinitionVersion } from "core/api";
import { shouldDisplayBreakingChangeBanner, getHumanReadableUpgradeDeadline } from "core/domain/connector";
import {
  ActorDefinitionVersionRead,
  FailureOrigin,
  FailureType,
  JobWithAttemptsRead,
} from "core/request/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./ConnectionStatusMessages.module.scss";

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

const reduceToHighestSeverityMessage = (messages: MessageProps[]): MessageProps[] => {
  // get the highest error type of all the messages e.g. error > warning > everything else
  const defaultMessageLevel = "info";
  const highestSeverityLevel = messages.reduce<MessageType>((highestSeverity: MessageType, message: MessageProps) => {
    const messageType = message.type ?? defaultMessageLevel;
    if (isHigherSeverity(messageType, highestSeverity)) {
      return messageType;
    }
    return highestSeverity;
  }, defaultMessageLevel);

  // filter out all messages that are not the highest severity level
  return messages.filter((message) => message.type === highestSeverityLevel);
};

/**
 * Get the error message to display for a given actor definition version
 * @param actorDefinitionVersion The actor definition version to get the error message for
 * @param connectorBreakingChangeDeadlinesEnabled
 * @returns An array containing id of the message to display and the type of error
 */
const getBreakingChangeErrorMessage = (
  actorDefinitionVersion: ActorDefinitionVersionRead,
  connectorBreakingChangeDeadlinesEnabled: boolean
): {
  errorMessageId: string;
  errorType: MessageType;
} => {
  // On OSS we do not pause connections for breaking changes, so we do not care about deadlines or unsupported versions
  if (!connectorBreakingChangeDeadlinesEnabled) {
    return { errorMessageId: "connectionForm.breakingChange.deprecatedNoDeadline.message", errorType: "warning" };
  }

  return actorDefinitionVersion.supportState === "unsupported"
    ? { errorMessageId: "connectionForm.breakingChange.unsupported.message", errorType: "error" }
    : { errorMessageId: "connectionForm.breakingChange.deprecated.message", errorType: "warning" };
};

export const ConnectionStatusMessages: React.FC = () => {
  const navigate = useNavigate();
  const { formatMessage } = useIntl();

  const workspaceId = useCurrentWorkspaceId();
  const { connection } = useConnectionEditService();
  const { lastCompletedSyncJob } = useConnectionSyncContext();
  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);
  const sourceActorDefinitionVersion = useSourceDefinitionVersion(connection.sourceId);
  const destinationActorDefinitionVersion = useDestinationDefinitionVersion(connection.destinationId);
  const connectorBreakingChangeDeadlinesEnabled = useFeature(FeatureItem.ConnectorBreakingChangeDeadlines);
  const [typeCount, setTypeCount] = useState<Partial<Record<MessageType, number>>>({});

  const errorMessagesToDisplay = useMemo<MessageProps[]>(() => {
    const errorMessages: MessageProps[] = [];

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
        const configError = {
          text: errorMessage,
          onAction: () => navigate(`/${RoutePaths.Workspaces}/${workspaceId}/${targetRoute}/${targetRouteId}`),
          actionBtnText: formatMessage({ id: "connection.stream.status.gotoSettings" }),
          type: "warning",
        } as const;

        errorMessages.push(configError);
      } else {
        const goToLogError = {
          text: errorMessage,
          onAction: () => navigate(`../${ConnectionRoutePaths.JobHistory}#${jobId}::${attemptId}`),
          actionBtnText: formatMessage({ id: "connection.stream.status.seeLogs" }),
          type: "warning",
        } as const;
        errorMessages.push(goToLogError);
      }
    }

    // If we have schema changes, show the correct message
    if (hasBreakingSchemaChange) {
      errorMessages.push({
        text: formatMessage({
          id: "connection.schemaChange.breaking",
        }),
        onAction: () => navigate(`../${ConnectionRoutePaths.Replication}`, { state: { triggerRefreshSchema: true } }),
        actionBtnText: formatMessage({ id: "connection.schemaChange.reviewAction" }),
        type: "error",
      });
    }

    // Warn the user of any breaking changes in the source definition
    const breakingChangeErrorMessages: MessageProps[] = [];
    if (shouldDisplayBreakingChangeBanner(sourceActorDefinitionVersion)) {
      const { errorMessageId, errorType } = getBreakingChangeErrorMessage(
        sourceActorDefinitionVersion,
        connectorBreakingChangeDeadlinesEnabled
      );

      breakingChangeErrorMessages.push({
        text: formatMessage(
          { id: errorMessageId },
          {
            actor_name: connection.source.name,
            actor_definition_name: connection.source.sourceName,
            actor_type: "source",
            connection_name: connection.name,
            upgrade_deadline: getHumanReadableUpgradeDeadline(sourceActorDefinitionVersion),
          }
        ),
        onAction: () =>
          navigate(`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Source}/${connection.sourceId}`),
        actionBtnText: formatMessage({
          id: "connectionForm.breakingChange.source.buttonLabel",
        }),
        actionBtnProps: {
          className: styles.breakingChangeButton,
        },
        type: errorType,
        iconOverride: "warning",
        "data-testid": `breaking-change-${errorType}-connection-banner`,
      });
    }

    // Warn the user of any breaking changes in the destination definition
    if (shouldDisplayBreakingChangeBanner(destinationActorDefinitionVersion)) {
      const { errorMessageId, errorType } = getBreakingChangeErrorMessage(
        destinationActorDefinitionVersion,
        connectorBreakingChangeDeadlinesEnabled
      );

      breakingChangeErrorMessages.push({
        text: formatMessage(
          { id: errorMessageId },
          {
            actor_name: connection.destination.name,
            actor_definition_name: connection.destination.destinationName,
            actor_type: "destination",
            connection_name: connection.name,
            upgrade_deadline: getHumanReadableUpgradeDeadline(destinationActorDefinitionVersion),
          }
        ),
        onAction: () =>
          navigate(`/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Destination}/${connection.destinationId}`),
        actionBtnText: formatMessage({
          id: "connectionForm.breakingChange.destination.buttonLabel",
        }),
        actionBtnProps: {
          className: styles.breakingChangeButton,
        },
        type: errorType,
        iconOverride: "warning",
        "data-testid": `breaking-change-${errorType}-connection-banner`,
      });
    }

    // If we have both source and destination breaking changes, with different error levels, we only
    // want to show the highest level error message
    const onlyHighLevelBreakingChangeErrorMessages = reduceToHighestSeverityMessage(breakingChangeErrorMessages);
    errorMessages.push(...onlyHighLevelBreakingChangeErrorMessages);

    // count the number of messages for each "type" and set the state
    setTypeCount(
      errorMessages.reduce<Partial<Record<MessageType, number>>>((acc, curr) => {
        if (curr.type) {
          acc[curr.type] = (acc[curr.type] || 0) + 1;
        }
        return acc;
      }, {})
    );

    // sort messages by severity level
    return errorMessages.sort((msg1, msg2) => {
      // since MessageProps.type is optional, we need to check for undefined
      if (!(msg1.type && msg2.type)) {
        return 0;
      }
      return MESSAGE_SEVERITY_LEVELS[msg2?.type] - MESSAGE_SEVERITY_LEVELS[msg1?.type];
    });
  }, [
    formatMessage,
    hasBreakingSchemaChange,
    lastCompletedSyncJob,
    navigate,
    connection.sourceId,
    connection.destinationId,
    workspaceId,
    sourceActorDefinitionVersion,
    destinationActorDefinitionVersion,
    connection.name,
    connection.source.name,
    connection.destination.name,
    connection.source.sourceName,
    connection.destination.destinationName,
    connectorBreakingChangeDeadlinesEnabled,
  ]);

  if (errorMessagesToDisplay.length > 0) {
    return (
      <Box p="lg">
        <FlexContainer
          direction="column"
          data-error-count={typeCount.error}
          data-warning-count={typeCount.warning}
          data-notification-count={typeCount.info}
        >
          {errorMessagesToDisplay.map((message, index) => (
            <Message key={index} className={styles.error} {...message} />
          ))}
        </FlexContainer>
      </Box>
    );
  }

  return null;
};
