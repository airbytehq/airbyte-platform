import classNames from "classnames";
import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Text } from "components/ui/Text";

import { useIsDataActivationConnection } from "area/connection/utils/useIsDataActivationConnection";
import { useCurrentConnection, useDestinationDefinitionVersion } from "core/api";
import {
  AirbyteStreamAndConfiguration,
  ConnectionStatus,
  DestinationSyncMode,
  SyncMode,
} from "core/api/types/AirbyteClient";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useModalService } from "hooks/services/Modal";
import { ConnectionRoutePaths } from "pages/routePaths";

import styles from "./StreamActionsMenu.module.scss";
import { ConnectionRefreshModal } from "../ConnectionSettingsPage/ConnectionRefreshModal";

interface StreamActionsMenuProps {
  streamName: string;
  streamNamespace?: string;
  catalogStream?: AirbyteStreamAndConfiguration;
}

export const StreamActionsMenu: React.FC<StreamActionsMenuProps> = ({ streamName, streamNamespace, catalogStream }) => {
  const canSyncConnection = useGeneratedIntent(Intent.RunAndCancelConnectionSyncAndRefresh);
  const { formatMessage } = useIntl();
  const navigate = useNavigate();
  const connection = useCurrentConnection();
  const { isSyncConnectionAvailable, clearStreams: resetStreams, refreshStreams } = useConnectionSyncContext();
  const isDataActivationConnection = useIsDataActivationConnection();
  const { supportsRefreshes: destinationSupportsRefreshes } = useDestinationDefinitionVersion(
    connection.destination.destinationId
  );
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { openModal } = useModalService();

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

  const hasIncremental = canMerge || canTruncate;

  if (!catalogStream) {
    return null;
  }

  const options: DropdownMenuOptionType[] = [
    {
      displayName: formatMessage({ id: "connection.stream.actions.edit" }),
      value: "editStream",
    },

    ...(isDataActivationConnection
      ? []
      : [
          {
            displayName: formatMessage({ id: "connection.stream.actions.refreshStream" }),
            value: "refreshStream",
            disabled: disableSyncActions || !destinationSupportsRefreshes || !hasIncremental,
            tooltipContent: !destinationSupportsRefreshes
              ? formatMessage({ id: "connection.stream.actions.refreshDisabled.destinationNotSupported" })
              : !hasIncremental
              ? formatMessage({ id: "connection.stream.actions.refreshDisabled.streamNotIncremental" })
              : undefined,
          },
        ]),
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
        state: { namespace: catalogStream.stream?.namespace, streamName: catalogStream.stream?.name, action: value },
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
