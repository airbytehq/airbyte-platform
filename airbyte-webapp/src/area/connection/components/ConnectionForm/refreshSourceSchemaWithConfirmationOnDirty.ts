import { useCallback } from "react";

import { useConnectionFormService } from "area/connection/utils/ConnectionForm/ConnectionFormService";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useFormChangeTrackerService } from "core/services/FormChangeTracker";

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
