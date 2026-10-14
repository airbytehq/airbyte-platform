import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Switch } from "components/ui/Switch";
import { Tooltip } from "components/ui/Tooltip";

import { useConnectionSemanticSearchEnablement, useUpdateConnectionSemanticSearchEnablement } from "core/api";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useNotificationService } from "core/services/Notification";
import { useIntent } from "core/utils/rbac";

interface SemanticSearchSwitchProps {
  connectionId: string;
  name: string;
  // The workspace that owns the connection. The edit permission is checked for this workspace.
  workspaceId: string;
  id?: string;
  size?: "lg" | "sm";
  disabled?: boolean;
}

/**
 * Shows and saves the semantic search setting of one connection. A change saves at once. Turning it off asks for
 * confirmation first, because the server deletes the semantic index.
 */
const SemanticSearchSwitchContent: React.FC<SemanticSearchSwitchProps> = ({
  connectionId,
  name,
  workspaceId,
  id,
  size = "sm",
  disabled = false,
}) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const canEdit = useIntent("EditConnectionSemanticSearch", { workspaceId });
  const enablementQuery = useConnectionSemanticSearchEnablement(connectionId);
  const updateEnablement = useUpdateConnectionSemanticSearchEnablement(connectionId);
  const enabled = enablementQuery.data?.enable_indexing ?? false;

  const saveEnablement = async (nextEnabled: boolean) => {
    try {
      await updateEnablement.mutateAsync({ enabled: nextEnabled, expectedEnabled: enabled });
    } catch {
      registerNotification({
        id: "semantic-search-connection-toggle-error",
        text: formatMessage({ id: "cloud.contextLayer.semanticSearch.updateError" }),
        type: "error",
      });
    }
  };

  const openDisableConfirmation = () => {
    openConfirmationModal({
      title: "cloud.contextLayer.semanticSearch.disableConfirm.title",
      text: "cloud.contextLayer.semanticSearch.disableConfirm.text",
      submitButtonText: "cloud.contextLayer.semanticSearch.disableConfirm.submit",
      submitButtonVariant: "danger",
      submitButtonDataId: "semantic-search-connection-disable-confirm",
      onSubmit: async () => {
        await saveEnablement(false);
        // Close the dialog even when the save fails. The update hook reads the saved value again, so the dialog
        // may no longer match it.
        closeConfirmationModal();
      },
    });
  };

  if (enablementQuery.isError) {
    return (
      <Button type="button" size="xs" variant="secondary" onClick={() => enablementQuery.refetch()}>
        <FormattedMessage id="form.tryAgain" />
      </Button>
    );
  }

  const control = (
    <Switch
      id={id}
      size={size}
      checked={enabled}
      disabled={!canEdit || disabled || enablementQuery.isLoading || updateEnablement.isLoading}
      onChange={enabled ? openDisableConfirmation : () => void saveEnablement(true)}
      aria-label={formatMessage({ id: "cloud.contextLayer.semanticSearchPage.semanticSearchControlLabel" }, { name })}
    />
  );

  if (!canEdit) {
    return (
      <Tooltip placement="bottom" control={control}>
        <FormattedMessage id="cloud.contextLayer.semanticSearch.connectionNoPermission" />
      </Tooltip>
    );
  }

  return control;
};

// The permission check can suspend while it reads the workspace, so keep that local to the switch. The fallback is a
// disabled switch, so a table cell is never blank.
const SemanticSearchSwitchFallback: React.FC<Pick<SemanticSearchSwitchProps, "id" | "name" | "size">> = ({
  id,
  name,
  size = "sm",
}) => {
  const { formatMessage } = useIntl();

  return (
    <Switch
      id={id}
      size={size}
      checked={false}
      disabled
      aria-label={formatMessage({ id: "cloud.contextLayer.semanticSearchPage.semanticSearchControlLabel" }, { name })}
    />
  );
};

export const SemanticSearchSwitch: React.FC<SemanticSearchSwitchProps> = (props) => (
  <React.Suspense fallback={<SemanticSearchSwitchFallback {...props} />}>
    <SemanticSearchSwitchContent {...props} />
  </React.Suspense>
);
