import classNames from "classnames";
import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { StreamWithStatus } from "components/connection/StreamStatus/streamStatusUtils";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Text } from "components/ui/Text";

import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";
import { ConnectionRefreshStreamModal } from "pages/connections/StreamStatusPage/ConnectionRefreshStreamModal";
import { ConnectionRoutePaths } from "pages/routePaths";

import styles from "./StreamActionsMenu.module.scss";

interface StreamActionsMenuProps {
  streamState: StreamWithStatus;
}

export const StreamActionsMenu: React.FC<StreamActionsMenuProps> = ({ streamState }) => {
  const { formatMessage } = useIntl();
  const navigate = useNavigate();
  const sayClearInsteadOfReset = useExperiment("connection.clearNotReset", false);
  const newRefreshTypes = useExperiment("platform.activate-refreshes", false);
  const destinationSupportsTruncateRefreshes = false; // for local testing.  this will be flagged on _only_ for a dev destination starting later in q1b.
  const destinationSupportsMergeRefreshes = false; // for local testing.  this will be flagged on _only_ for a dev destination starting later in q1b.

  const { syncStarting, jobSyncRunning, resetStarting, jobResetRunning, resetStreams, refreshStreams } =
    useConnectionSyncContext();
  const { mode, connection } = useConnectionFormService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { openModal } = useModalService();

  const catalogStream = connection.syncCatalog.streams.find(
    (catalogStream) =>
      catalogStream.stream?.name === streamState.streamName &&
      catalogStream.stream?.namespace === streamState.streamNamespace
  );

  /**
   * In order to refresh a stream, both the destination AND the sync mode must support one of the refresh modes
   * Currently, destination support is simply hardcoded in lines 32-33.  However, we will be moving to a feature flag
   * and/or destination metadata support before release.
   */
  const { canMerge, canTruncate } = useMemo(() => {
    const hasIncremental = catalogStream?.config?.syncMode === SyncMode.incremental;
    const hasAppendDedupe = catalogStream?.config?.destinationSyncMode === DestinationSyncMode.append_dedup;

    return {
      canMerge: hasIncremental && destinationSupportsMergeRefreshes,
      canTruncate: hasIncremental && hasAppendDedupe && destinationSupportsTruncateRefreshes,
    };
  }, [
    catalogStream?.config?.destinationSyncMode,
    catalogStream?.config?.syncMode,
    destinationSupportsMergeRefreshes,
    destinationSupportsTruncateRefreshes,
  ]);

  // the platform must support refresh operations AND the stream must support at least one of the refresh types
  const showRefreshOption = newRefreshTypes && (canMerge || canTruncate);

  if (!catalogStream) {
    return null;
  }

  const options: DropdownMenuOptionType[] = [
    ...(sayClearInsteadOfReset
      ? []
      : [
          {
            displayName: formatMessage({ id: "connection.stream.actions.resetThisStream" }),
            value: "resetThisStream",
            disabled: syncStarting || jobSyncRunning || resetStarting || jobResetRunning || mode === "readonly",
          },
        ]),
    {
      displayName: formatMessage({ id: "connection.stream.actions.showInReplicationTable" }),
      value: "showInReplicationTable",
    },
    {
      displayName: formatMessage({ id: "connection.stream.actions.openDetails" }),
      value: "openDetails",
    },
    ...(showRefreshOption
      ? [
          {
            displayName: formatMessage({ id: "connection.stream.actions.refreshStream" }),
            value: "refreshStream",
            disabled: syncStarting || jobSyncRunning || resetStarting || jobResetRunning || mode === "readonly",
          },
        ]
      : []),
    ...(!sayClearInsteadOfReset
      ? []
      : [
          {
            displayName: formatMessage({
              id: "connection.stream.actions.clearData",
            }),
            value: "clearStreamData",
            disabled: syncStarting || jobSyncRunning || resetStarting || jobResetRunning || mode === "readonly",
            className: classNames(styles.streamActionsMenu__clearDataLabel),
          },
        ]),
  ];

  const onOptionClick = async ({ value }: DropdownMenuOptionType) => {
    if (value === "showInReplicationTable" || value === "openDetails") {
      navigate(`../${ConnectionRoutePaths.Replication}`, {
        state: { namespace: streamState?.streamNamespace, streamName: streamState?.streamName, action: value },
      });
    }

    if (value === "clearStreamData") {
      openConfirmationModal({
        title: (
          <FormattedMessage
            id="connection.stream.actions.clearData.confirm.title"
            values={{
              streamName: (
                <span className={styles.streamActionsMenu__clearDataModalStreamName}>{streamState.streamName}</span>
              ),
            }}
          />
        ),
        text: "connection.stream.actions.clearData.confirm.text",
        additionalContent: (
          <Box pt="xl">
            <Text color="grey400">
              <FormattedMessage id="connection.stream.actions.clearData.confirm.additionalText" />
            </Text>
          </Box>
        ),
        submitButtonText: "connection.stream.actions.clearData.confirm.submit",
        cancelButtonText: "connection.stream.actions.clearData.confirm.cancel",
        onSubmit: async () => {
          await resetStreams([{ streamNamespace: streamState.streamNamespace, streamName: streamState.streamName }]);
          closeConfirmationModal();
        },
      });
    }

    if (value === "refreshStream") {
      openModal<void>({
        size: "md",
        title: (
          <FormattedMessage
            id="connection.stream.actions.refreshStream.confirm.title"
            values={{
              streamName: (
                <span className={styles.streamActionsMenu__clearDataModalStreamName}>{streamState.streamName}</span>
              ),
            }}
          />
        ),
        content: ({ onComplete, onCancel }) => {
          return (
            <ConnectionRefreshStreamModal
              onComplete={onComplete}
              onCancel={onCancel}
              canTruncate={canTruncate}
              canMerge={canMerge}
              streamNamespace={streamState.streamNamespace}
              streamName={streamState.streamName}
              refreshStreams={refreshStreams}
            />
          );
        },
      });
    }

    if (value === "resetThisStream" && streamState) {
      await resetStreams([{ streamNamespace: streamState.streamNamespace, streamName: streamState.streamName }]);
    }
  };

  return (
    <DropdownMenu placement="bottom-end" options={options} onChange={onOptionClick}>
      {() => <Button variant="clear" icon="options" />}
    </DropdownMenu>
  );
};
