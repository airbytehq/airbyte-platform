import classNames from "classnames";
import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Text } from "components/ui/Text";

import { useCurrentConnection, useDestinationDefinitionVersion } from "core/api";
import { ConnectionStatus, DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";
import { ConnectionRoutePaths } from "pages/routePaths";

import styles from "./StreamActionsMenu.module.scss";
import { ConnectionRefreshModal } from "../ConnectionSettingsPage/ConnectionRefreshModal";

interface StreamActionsMenuProps {
  streamName: string;
  streamNamespace?: string;
}

export const StreamActionsMenu: React.FC<StreamActionsMenuProps> = ({ streamName, streamNamespace }) => {
  const isSyncCatalogV2Enabled = useExperiment("connection.syncCatalogV2");
  const isSyncCatalogV2Allowed = useFeature(FeatureItem.SyncCatalogV2);
  const useSyncCatalogV2 = isSyncCatalogV2Enabled && isSyncCatalogV2Allowed;
  const canSyncConnection = useGeneratedIntent(Intent.RunAndCancelConnectionSyncAndRefresh);
  const { formatMessage } = useIntl();
  const navigate = useNavigate();
  const connection = useCurrentConnection();
  const { isSyncConnectionAvailable, clearStreams: resetStreams, refreshStreams } = useConnectionSyncContext();

  const { supportsRefreshes: destinationSupportsRefreshes } = useDestinationDefinitionVersion(
    connection.destination.destinationId
  );
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { openModal } = useModalService();

  const catalogStream = connection.syncCatalog.streams.find(
    (catalogStream) => catalogStream.stream?.name === streamName && catalogStream.stream?.namespace === streamNamespace
  );

  const disableSyncActions =
    !isSyncConnectionAvailable || connection.status !== ConnectionStatus.active || !canSyncConnection;

  const { canMerge, canTruncate } = useMemo(() => {
    const hasIncremental = catalogStream?.config?.syncMode === SyncMode.incremental;
    const hasAppendDedupe = catalogStream?.config?.destinationSyncMode === DestinationSyncMode.append_dedup;

    return {
      canMerge: hasIncremental && destinationSupportsRefreshes,
      canTruncate: hasIncremental && hasAppendDedupe && destinationSupportsRefreshes,
    };
  }, [catalogStream?.config?.destinationSyncMode, catalogStream?.config?.syncMode, destinationSupportsRefreshes]);

  const showRefreshOption = canMerge || canTruncate;

  if (!catalogStream) {
    return null;
  }

  const options: DropdownMenuOptionType[] = [
    ...(useSyncCatalogV2
      ? [
          {
            displayName: formatMessage({ id: "connection.stream.actions.edit" }),
            value: "editStream",
          },
        ]
      : [
          {
            displayName: formatMessage({ id: "connection.stream.actions.showInReplicationTable" }),
            value: "showInReplicationTable",
          },
          {
            displayName: formatMessage({ id: "connection.stream.actions.openDetails" }),
            value: "openDetails",
          },
        ]),
    ...(showRefreshOption
      ? [
          {
            displayName: formatMessage({ id: "connection.stream.actions.refreshStream" }),
            value: "refreshStream",
            disabled: disableSyncActions,
          },
        ]
      : []),
    {
      displayName: formatMessage({
        id: "connection.stream.actions.clearData",
      }),
      value: "clearStreamData",
      disabled: disableSyncActions,
      className: classNames(styles.streamActionsMenu__clearDataLabel),
    },
  ];

  const onOptionClick = async ({ value }: DropdownMenuOptionType) => {
    if (value === "showInReplicationTable" || value === "openDetails" || value === "editStream") {
      navigate(`../${ConnectionRoutePaths.Replication}`, {
        state: { namespace: streamNamespace, streamName, action: value },
      });
    }

    if (value === "refreshStream") {
      openModal<void>({
        size: "md",
        title: (
          <FormattedMessage
            id="connection.stream.actions.refreshStream.confirm.title"
            values={{
              streamName: <span className={styles.streamActionsMenu__clearDataModalStreamName}>{streamName}</span>,
            }}
          />
        ),
        content: ({ onComplete, onCancel }) => {
          return (
            <ConnectionRefreshModal
              refreshScope="stream"
              onComplete={onComplete}
              onCancel={onCancel}
              streamsSupportingMergeRefresh={canMerge ? [{ streamNamespace, streamName }] : []}
              streamsSupportingTruncateRefresh={canTruncate ? [{ streamNamespace, streamName }] : []}
              refreshStreams={refreshStreams}
            />
          );
        },
      });
    }

    if (value === "clearStreamData") {
      openConfirmationModal({
        title: (
          <FormattedMessage
            id="connection.stream.actions.clearData.confirm.title"
            values={{
              streamName: <span className={styles.streamActionsMenu__clearDataModalStreamName}>{streamName}</span>,
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
          await resetStreams([{ streamNamespace, streamName }]);
          closeConfirmationModal();
        },
      });
    }
  };

  return (
    <DropdownMenu placement="bottom-end" options={options} onChange={onOptionClick}>
      {() => <Button variant="clear" icon="options" />}
    </DropdownMenu>
  );
};
