import React, { useCallback } from "react";

import { Blocker, useBlocker } from "core/services/navigation";

import { useFormChangeTrackerService } from "./hooks";
import { useConfirmationModalService } from "../ConfirmationModal";

export const FormChangeTrackerService: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { hasFormChanges, clearAllFormChanges } = useFormChangeTrackerService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const blocker = useCallback(
    (blocker: Blocker) => {
      openConfirmationModal({
        title: "form.unsavedChangesTitle",
        text: "form.unsavedChangesMessage",
        submitButtonText: "form.leavePage",
        onSubmit: () => {
          clearAllFormChanges();
          closeConfirmationModal();
          blocker.proceed();
        },
      });
    },
    [clearAllFormChanges, closeConfirmationModal, openConfirmationModal]
  );

  useBlocker(blocker, hasFormChanges);

  return <>{children}</>;
};
