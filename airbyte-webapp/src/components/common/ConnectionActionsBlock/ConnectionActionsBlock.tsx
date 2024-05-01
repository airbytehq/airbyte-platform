import React, { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useDeleteConnection, useResetConnection } from "core/api";
import { ConnectionStatus } from "core/api/types/AirbyteClient";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";
import { useDeleteModal } from "hooks/useDeleteModal";
import { ConnectionRefreshModal } from "pages/connections/ConnectionSettingsPage/ConnectionRefreshModal";

export const ConnectionActionsBlock: React.FC = () => {
  const { mode } = useConnectionFormService();
  const { connection, streamsByRefreshType } = useConnectionEditService();
  const { refreshStreams } = useConnectionSyncContext();
  const { openModal } = useModalService();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const isRefreshEnabled = useExperiment("platform.activate-refreshes", false);

  const { mutateAsync: deleteConnection } = useDeleteConnection();
  const onDelete = () => deleteConnection(connection);

  const { mutateAsync: doResetConnection } = useResetConnection();
  const onReset = useCallback(async () => {
    await doResetConnection(connection.connectionId);
    registerNotification({
      id: "clearData.successfulStart",
      text: formatMessage({
        id: "form.clearData.successfulStart",
      }),
      type: "success",
    });
  }, [doResetConnection, connection.connectionId, registerNotification, formatMessage]);

  const onDeleteButtonClick = useDeleteModal("connection", onDelete, undefined, connection?.name);
  const connectionStatus = useConnectionStatus(connection.connectionId ?? "");

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const resetWithModal = useCallback(() => {
    openConfirmationModal({
      title: "connection.actions.clearData.confirm.title",
      text: "connection.actions.clearData.confirm.text",
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
        await onReset();
        closeConfirmationModal();
      },
    });
  }, [closeConfirmationModal, onReset, openConfirmationModal]);

  const refreshWithModal = useCallback(() => {
    openModal<void>({
      size: "md",
      title: <FormattedMessage id="connection.actions.refreshData.confirm.title" />,

      content: ({ onComplete, onCancel }) => {
        return (
          <ConnectionRefreshModal
            refreshScope="connection"
            onComplete={onComplete}
            onCancel={onCancel}
            streamsSupportingMergeRefresh={streamsByRefreshType.streamsSupportingMergeRefresh}
            streamsSupportingTruncateRefresh={streamsByRefreshType.streamsSupportingTruncateRefresh}
            refreshStreams={refreshStreams}
            totalEnabledStreams={
              connection.syncCatalog.streams.filter((stream) => stream.config?.selected === true).length
            }
          />
        );
      },
    });
  }, [
    connection.syncCatalog.streams,
    openModal,
    refreshStreams,
    streamsByRefreshType.streamsSupportingMergeRefresh,
    streamsByRefreshType.streamsSupportingTruncateRefresh,
  ]);

  const onResetButtonClick = () => {
    resetWithModal();
  };

  const onRefreshModalClick = () => {
    refreshWithModal();
  };

  const RefreshConnectionButton = () => {
    return (
      <Button
        variant="secondary"
        onClick={onRefreshModalClick}
        data-id="open-refresh-modal"
        disabled={
          (streamsByRefreshType.streamsSupportingMergeRefresh.length === 0 &&
            streamsByRefreshType.streamsSupportingTruncateRefresh.length === 0) ||
          mode === "readonly" ||
          connectionStatus.isRunning ||
          connection.status !== ConnectionStatus.active
        }
      >
        <FormattedMessage id="connection.actions.refreshData" />
      </Button>
    );
  };

  return (
    <Card>
      <FlexContainer direction="column" gap="xl">
        {isRefreshEnabled && (
          <FormFieldLayout alignItems="center" nextSizing>
            <FlexContainer direction="column" gap="xs">
              <Text size="lg">
                <FormattedMessage id="connection.actions.refreshData" />
              </Text>
              <Text size="xs" color="grey">
                <FormattedMessage id="connection.actions.refreshData.description" />
              </Text>
            </FlexContainer>
            {streamsByRefreshType.streamsSupportingMergeRefresh.length > 0 ||
            streamsByRefreshType.streamsSupportingTruncateRefresh.length > 0 ? (
              <RefreshConnectionButton />
            ) : (
              <Tooltip control={<RefreshConnectionButton />}>
                <FormattedMessage id="connection.actions.refreshData.notAvailable" />
              </Tooltip>
            )}
          </FormFieldLayout>
        )}

        <FormFieldLayout alignItems="center" nextSizing>
          <FlexContainer direction="column" gap="xs">
            <Text size="lg">
              <FormattedMessage id="connection.actions.clearYourData" />
            </Text>
            <Text size="xs" color="grey">
              <FormattedMessage id="connection.actions.clearDataDescription" />
            </Text>
          </FlexContainer>
          <Button
            variant="secondary"
            onClick={onResetButtonClick}
            data-id="open-reset-modal"
            disabled={
              mode === "readonly" || connectionStatus.isRunning || connection.status !== ConnectionStatus.active
            }
          >
            <FormattedMessage id="connection.actions.clearYourData" />
          </Button>
        </FormFieldLayout>

        <FormFieldLayout alignItems="center" nextSizing>
          <FlexContainer direction="column" gap="xs">
            <Text size="lg">
              <FormattedMessage id="tables.connectionDelete.title" />
            </Text>
            <Text size="xs" color="grey">
              <FormattedMessage id="tables.connectionDataDelete" />
            </Text>
          </FlexContainer>
          <Button
            variant="danger"
            onClick={onDeleteButtonClick}
            data-id="open-delete-modal"
            disabled={mode === "readonly"}
          >
            <FormattedMessage id="tables.connectionDelete" />
          </Button>
        </FormFieldLayout>
      </FlexContainer>
    </Card>
  );
};
