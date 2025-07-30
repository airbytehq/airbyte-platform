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

import { useIsDataActivationConnection } from "area/connection/utils/useIsDataActivationConnection";
import { useDeleteConnection, useDestinationDefinitionVersion } from "core/api";
import { ConnectionStatus, ConnectionSyncStatus } from "core/api/types/AirbyteClient";
import { useFormMode } from "core/services/ui/FormModeContext";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";
import { useDeleteModal } from "hooks/useDeleteModal";
import { ConnectionRefreshModal } from "pages/connections/ConnectionSettingsPage/ConnectionRefreshModal";

export const ConnectionActionsBlock: React.FC = () => {
  const { mode } = useFormMode();
  const { connection, streamsByRefreshType } = useConnectionEditService();
  const canSyncConnection = useGeneratedIntent(Intent.RunAndCancelConnectionSyncAndRefresh);
  const canEditConnection = useGeneratedIntent(Intent.CreateOrEditConnection);
  const { refreshStreams } = useConnectionSyncContext();
  const { openModal } = useModalService();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const { supportsRefreshes: destinationSupportsRefreshes } = useDestinationDefinitionVersion(
    connection.destination.destinationId
  );
  const { clearStreams } = useConnectionSyncContext();
  const { mutateAsync: deleteConnection } = useDeleteConnection();
  const onDelete = () => deleteConnection(connection);
  const isDataActivationConnection = useIsDataActivationConnection();

  const onReset = useCallback(async () => {
    await clearStreams();
    registerNotification({
      id: "clearData.successfulStart",
      text: formatMessage({
        id: "form.clearData.successfulStart",
      }),
      type: "success",
    });
  }, [clearStreams, registerNotification, formatMessage]);

  const onDeleteButtonClick = useDeleteModal(
    "connection",
    onDelete,
    undefined,
    formatMessage({ id: "tables.connectionDeleteConfirmationText" })
  );
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

  const ClearConnectionDataButton = () => {
    const tooltipContent =
      mode === "readonly"
        ? undefined
        : connection.status === ConnectionStatus.inactive
        ? "connection.actions.clearYourData.disabledConnectionTooltip"
        : connectionStatus.status === ConnectionSyncStatus.running
        ? "connection.actions.clearYourData.runningJobTooltip"
        : undefined;

    const ClearButton = () => {
      return (
        <Button
          variant="secondary"
          onClick={onResetButtonClick}
          data-id="open-reset-modal"
          disabled={mode === "readonly" || !!tooltipContent}
        >
          <FormattedMessage id="connection.actions.clearYourData" />
        </Button>
      );
    };

    return tooltipContent ? (
      <Tooltip control={<ClearButton />}>
        <FormattedMessage id={tooltipContent} />
      </Tooltip>
    ) : (
      <ClearButton />
    );
  };

  const RefreshConnectionDataButton = () => {
    const tooltipContent =
      connection.status === ConnectionStatus.deprecated
        ? undefined
        : destinationSupportsRefreshes === false
        ? "connection.actions.refreshData.notAvailable.destination"
        : streamsByRefreshType.streamsSupportingMergeRefresh.length === 0 &&
          streamsByRefreshType.streamsSupportingTruncateRefresh.length === 0
        ? "connection.actions.refreshData.notAvailable.streams"
        : connection.status !== ConnectionStatus.active
        ? "connection.actions.refreshData.disabledConnectionTooltip"
        : connectionStatus.status === ConnectionSyncStatus.running
        ? "connection.actions.refreshData.runningJobTooltip"
        : undefined;

    const RefreshConnectionButton = () => {
      return (
        <Button
          variant="secondary"
          onClick={onRefreshModalClick}
          data-id="open-refresh-modal"
          disabled={!!tooltipContent || connection.status === ConnectionStatus.deprecated || !canSyncConnection}
        >
          <FormattedMessage id="connection.actions.refreshData" />
        </Button>
      );
    };

    return tooltipContent ? (
      <Tooltip control={<RefreshConnectionButton />}>
        <FormattedMessage id={tooltipContent} />
      </Tooltip>
    ) : (
      <RefreshConnectionButton />
    );
  };

  return (
    <Card>
      <FlexContainer direction="column" gap="xl">
        {!isDataActivationConnection && (
          <FormFieldLayout alignItems="center" nextSizing>
            <FlexContainer direction="column" gap="xs">
              <Text size="lg">
                <FormattedMessage id="connection.actions.refreshData" />
              </Text>
              <Text size="xs" color="grey">
                <FormattedMessage id="connection.actions.refreshData.description" />
              </Text>
            </FlexContainer>
            <RefreshConnectionDataButton />
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
          <ClearConnectionDataButton />
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
            disabled={!canEditConnection} // "mode" of the connection is set by three things: (1) connection being deleted or not (2) RBAC to edit the connection (3) having an unlicensed connector.  The only one that matters here is (2).
          >
            <FormattedMessage id="tables.connectionDelete" />
          </Button>
        </FormFieldLayout>
      </FlexContainer>
    </Card>
  );
};
