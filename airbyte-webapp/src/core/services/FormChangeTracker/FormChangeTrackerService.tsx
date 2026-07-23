import React, { useCallback } from "react";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { Blocker, useBlocker } from "core/services/navigation";

import { isGeneratedFormId, useFormChangeTrackerService } from "./hooks";
import { useConfirmationModalService } from "../ConfirmationModal";

export const FormChangeTrackerService: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { hasFormChanges, clearAllFormChanges, getDirtyFormIds } = useFormChangeTrackerService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const analyticsService = useAnalyticsService();
  const blocker = useCallback(
    (blocker: Blocker) => {
      openConfirmationModal({
        title: "form.unsavedChangesTitle",
        text: "form.unsavedChangesMessage",
        submitButtonText: "form.leavePage",
        onSubmit: () => {
          const dirtyFormIds = getDirtyFormIds().filter((id) => !isGeneratedFormId(id));
          if (dirtyFormIds.length) {
            analyticsService.track(Namespace.FORM, Action.DISMISSED_CHANGES_MODAL, {
              actionDescription: "User dismissed the leave changes modal",
              dirtyFormIds,
            });
          }

          clearAllFormChanges();
          closeConfirmationModal();
          blocker.proceed();
        },
      });
    },
    [clearAllFormChanges, closeConfirmationModal, openConfirmationModal, getDirtyFormIds, analyticsService]
  );

  useBlocker(blocker, hasFormChanges);

  return <>{children}</>;
};
