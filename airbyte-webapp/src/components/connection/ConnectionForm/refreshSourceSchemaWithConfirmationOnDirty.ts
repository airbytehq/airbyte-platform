import { useCallback } from "react";

import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";

export const useRefreshSourceSchemaWithConfirmationOnDirty = (dirty: boolean) => {
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { refreshSchema } = useConnectionFormService();

  return useCallback(() => {
    if (dirty) {
      openConfirmationModal({
        title: "connection.updateSchema.formChanged.title",
        text: "connection.updateSchema.formChanged.text",
        submitButtonText: "connection.updateSchema.formChanged.confirm",
        onSubmit: () => {
          closeConfirmationModal();
          clearAllFormChanges();
          refreshSchema();
        },
      });
    } else {
      refreshSchema();
    }
  }, [clearAllFormChanges, closeConfirmationModal, dirty, openConfirmationModal, refreshSchema]);
};
