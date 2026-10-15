import React, { createContext, useCallback, useContext, useMemo, useRef, useState } from "react";
import { FormattedMessage, FormattedNumber, useIntl } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import DatePicker from "components/ui/DatePicker";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useConnectionSemanticSearchEnablement, useRequestConnectionBackfill, useSourceSearchStatuses } from "core/api";
import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { useNotificationService } from "core/services/Notification";
import { useIntent } from "core/utils/rbac";

import {
  BACKFILL_STATUS_POLL_INTERVAL_MS,
  BackfillRequest,
  getBackfillRequestState,
  getDefaultBackfillStartTime,
  getRowBackfillState,
  hasRunningBackfill,
  isValidBackfillStartTime,
  SourceStatusRead,
} from "./connectionBackfillState";
import styles from "./SemanticSearchPage.module.scss";

// The connection fields that the backfill controls read. A connection list item and a full connection read both have
// them.
type BackfillConnection = Pick<WebBackendConnectionListItem, "connectionId" | "name"> & {
  source: Pick<WebBackendConnectionListItem["source"], "sourceId">;
};

interface SemanticSearchBackfillContextValue {
  statusBySourceId: Map<string, SourceStatusRead>;
  defaultStartTime: string;
  startTimes: Record<string, string>;
  setStartTime: (connectionId: string, startTime: string) => void;
  requests: Record<string, BackfillRequest>;
  setRequest: (connectionId: string, request: BackfillRequest) => void;
}

const SemanticSearchBackfillContext = createContext<SemanticSearchBackfillContextValue | null>(null);

const useSemanticSearchBackfillContext = () => {
  const context = useContext(SemanticSearchBackfillContext);
  if (!context) {
    throw new Error("useSemanticSearchBackfillContext must be used within a SemanticSearchBackfillProvider.");
  }
  return context;
};

/**
 * Holds the backfill state of the connections on one page: the source status reads, the start times that the user
 * entered, and the outcome of each start request. The start time is sent with the request only and is not saved. An
 * empty start time sends the default, which stays fixed while the page is open.
 */
export const SemanticSearchBackfillProvider: React.FC<
  React.PropsWithChildren<{ connections: BackfillConnection[] }>
> = ({ connections, children }) => {
  const sourceIds = useMemo(
    () => Array.from(new Set(connections.map((connection) => connection.source.sourceId))),
    [connections]
  );
  const connectionIds = useMemo(() => connections.map((connection) => connection.connectionId), [connections]);
  const statuses = useSourceSearchStatuses(sourceIds, {
    refetchInterval: (data, query) =>
      query.state.status === "error" || hasRunningBackfill(data, connectionIds)
        ? BACKFILL_STATUS_POLL_INTERVAL_MS
        : false,
  });

  const defaultStartTime = useMemo(() => getDefaultBackfillStartTime(), []);
  const [startTimes, setStartTimes] = useState<Record<string, string>>({});
  const setStartTime = useCallback(
    (connectionId: string, startTime: string) =>
      setStartTimes((current) => ({ ...current, [connectionId]: startTime })),
    []
  );

  const [requests, setRequests] = useState<Record<string, BackfillRequest>>({});
  const setRequest = useCallback(
    (connectionId: string, request: BackfillRequest) =>
      setRequests((current) => ({ ...current, [connectionId]: request })),
    []
  );

  // useQueries returns a new array on every render. The map keeps only the fields that rows read, and the key covers
  // all of them, because data changes only together with dataUpdatedAt.
  const statusReads: SourceStatusRead[] = statuses.map(({ data, dataUpdatedAt, isError }) => ({
    data,
    dataUpdatedAt,
    isError,
  }));
  const statusReadsKey = statusReads.map(({ dataUpdatedAt, isError }) => `${dataUpdatedAt}:${isError}`).join(",");
  const statusBySourceId = useMemo(
    () => new Map(sourceIds.map((sourceId, index) => [sourceId, statusReads[index]])),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [sourceIds, statusReadsKey]
  );

  const value = useMemo(
    () => ({
      statusBySourceId,
      defaultStartTime,
      startTimes,
      setStartTime,
      requests,
      setRequest,
    }),
    [statusBySourceId, defaultStartTime, startTimes, setStartTime, requests, setRequest]
  );

  return <SemanticSearchBackfillContext.Provider value={value}>{children}</SemanticSearchBackfillContext.Provider>;
};

const useConnectionBackfillState = (connection: BackfillConnection) => {
  const { statusBySourceId, requests } = useSemanticSearchBackfillContext();
  const statusQuery = statusBySourceId.get(connection.source.sourceId);
  const dataUpdatedAt = statusQuery?.dataUpdatedAt ?? 0;
  const state = getRowBackfillState(connection.connectionId, statusQuery, requests[connection.connectionId]);

  // A running backfill keeps the start time it was requested with, and a new request would re-run its finished streams.
  // The latest read must succeed before the date or button unlock, because the last status may be out of date.
  const locked = state.status === "running" || state.status === "loading" || !!statusQuery?.isError;

  return { state, locked, dataUpdatedAt };
};

// The server rejects a backfill while the connection's semantic search is off.
const useCanEditBackfill = (connectionId: string, workspaceId: string) => {
  const canEdit = useIntent("EditConnectionSemanticSearch", { workspaceId });
  const enablementQuery = useConnectionSemanticSearchEnablement(connectionId);
  return canEdit && enablementQuery.data?.enable_indexing === true;
};

// disabled comes from the caller, for example a deprecated connection's settings.
const useBackfillControls = (connection: BackfillConnection, workspaceId: string, disabled = false) => {
  const canEditBackfill = useCanEditBackfill(connection.connectionId, workspaceId);
  const { locked, dataUpdatedAt } = useConnectionBackfillState(connection);
  return { controlsDisabled: disabled || !canEditBackfill || locked, dataUpdatedAt };
};

interface BackfillDateProps {
  id: string;
  connection: BackfillConnection;
  workspaceId: string;
  disabled?: boolean;
}

const BackfillDateContent: React.FC<BackfillDateProps> = ({ id, connection, workspaceId, disabled }) => {
  const { defaultStartTime, startTimes, setStartTime } = useSemanticSearchBackfillContext();
  const { controlsDisabled } = useBackfillControls(connection, workspaceId, disabled);
  const startTime = startTimes[connection.connectionId] ?? "";
  const invalid = !isValidBackfillStartTime(startTime);

  return (
    <FlexContainer direction="column" gap="xs">
      <DatePicker
        id={id}
        value={startTime}
        placeholder={defaultStartTime}
        onChange={(value) => setStartTime(connection.connectionId, value)}
        disabled={controlsDisabled}
        error={invalid}
        withTime
      />
      {invalid && (
        <Text color="red" size="xs">
          <FormattedMessage id="cloud.contextLayer.semanticSearchPage.backfillDateInvalid" />
        </Text>
      )}
    </FlexContainer>
  );
};

// The permission check can suspend while it reads the workspace. The fallback is a disabled control.
export const SemanticSearchBackfillDate: React.FC<BackfillDateProps> = (props) => (
  <React.Suspense fallback={<DatePicker id={props.id} value="" onChange={() => undefined} disabled withTime />}>
    <BackfillDateContent {...props} />
  </React.Suspense>
);

interface StartBackfillProps {
  connection: BackfillConnection;
  workspaceId: string;
  disabled?: boolean;
}

const StartBackfillButton: React.FC<{
  name: string;
  disabled: boolean;
  isLoading?: boolean;
  onClick?: () => void;
}> = ({ name, disabled, isLoading, onClick }) => {
  const { formatMessage } = useIntl();

  return (
    <Button
      type="button"
      variant="secondary"
      disabled={disabled}
      isLoading={isLoading}
      onClick={onClick}
      aria-label={formatMessage({ id: "cloud.contextLayer.semanticSearchPage.startBackfillControlLabel" }, { name })}
    >
      <FormattedMessage id="cloud.contextLayer.semanticSearchPage.startBackfill" />
    </Button>
  );
};

const StartBackfillContent: React.FC<StartBackfillProps> = ({ connection, workspaceId, disabled }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { defaultStartTime, startTimes, setRequest } = useSemanticSearchBackfillContext();
  const { controlsDisabled, dataUpdatedAt } = useBackfillControls(connection, workspaceId, disabled);
  const startTime = startTimes[connection.connectionId] ?? "";
  const requestBackfill = useRequestConnectionBackfill(connection.connectionId, connection.source.sourceId);
  // The request records the status version that was current when it finished, not when it started.
  const statusVersionRef = useRef(dataUpdatedAt);
  statusVersionRef.current = dataUpdatedAt;

  const startBackfill = async () => {
    try {
      const response = await requestBackfill.mutateAsync({
        startTime: startTime || defaultStartTime,
      });
      const requestState = getBackfillRequestState(response);
      if (requestState) {
        setRequest(connection.connectionId, {
          state: requestState,
          statusVersionAtRequest: statusVersionRef.current,
        });
      }
    } catch {
      registerNotification({
        id: "semantic-search-backfill-error",
        text: formatMessage({ id: "cloud.contextLayer.semanticSearchPage.backfillError" }),
        type: "error",
      });
    }
  };

  return (
    <StartBackfillButton
      name={connection.name}
      disabled={controlsDisabled || !isValidBackfillStartTime(startTime)}
      isLoading={requestBackfill.isLoading}
      onClick={() => void startBackfill()}
    />
  );
};

// The permission check can suspend while it reads the workspace. The fallback is a disabled control.
export const SemanticSearchStartBackfill: React.FC<StartBackfillProps> = (props) => (
  <React.Suspense fallback={<StartBackfillButton name={props.connection.name} disabled />}>
    <StartBackfillContent {...props} />
  </React.Suspense>
);

export const SemanticSearchBackfillStatus: React.FC<{ connection: BackfillConnection }> = ({ connection }) => {
  const { state } = useConnectionBackfillState(connection);

  switch (state.status) {
    case "loading":
      return <Icon type="loading" size="sm" color="disabled" />;
    case "unknown":
      return (
        <Tooltip
          placement="top"
          control={
            <Badge variant="grey" uppercase={false} className={styles.statusBadge}>
              <Icon type="warningFilled" size="xs" />
              <FormattedMessage id="cloud.contextLayer.semanticSearchPage.backfillUnknown" />
            </Badge>
          }
        >
          <FormattedMessage id="cloud.contextLayer.semanticSearchPage.backfillUnknown.tooltip" />
        </Tooltip>
      );
    case "running":
      return (
        <FlexContainer alignItems="center" gap="sm">
          <Badge variant="blue" uppercase={false} className={styles.statusBadge}>
            <Icon type="loading" size="xs" />
            <FormattedMessage id="cloud.contextLayer.semanticSearchPage.backfillRunning" />
          </Badge>
          {state.percent !== undefined && (
            <Badge variant="teal" uppercase={false} radius="2xs">
              <FormattedNumber value={state.percent / 100} style="percent" />
            </Badge>
          )}
        </FlexContainer>
      );
    case "complete":
      return (
        <Badge variant="teal" uppercase={false} className={styles.statusBadge}>
          <Icon type="successFilled" size="xs" />
          <FormattedMessage id="cloud.contextLayer.semanticSearchPage.backfillComplete" />
        </Badge>
      );
    case "failed":
      return (
        <Tooltip
          placement="top"
          control={
            <Badge variant="red" uppercase={false} className={styles.statusBadge}>
              <Icon type="errorFilled" size="xs" />
              <FormattedMessage id="cloud.contextLayer.semanticSearchPage.backfillFailed" />
            </Badge>
          }
        >
          <FormattedMessage id="cloud.contextLayer.semanticSearchPage.backfillFailed.tooltip" />
        </Tooltip>
      );
    case "off":
      return (
        <Badge variant="grey" uppercase={false} className={styles.statusBadge}>
          <Icon type="minusCircle" size="xs" />
          <FormattedMessage id="cloud.contextLayer.semanticSearchPage.backfillOff" />
        </Badge>
      );
  }
};
