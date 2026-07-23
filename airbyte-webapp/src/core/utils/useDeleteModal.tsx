import React, { useCallback } from "react";
import { useNavigate } from "react-router-dom";

import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { RoutePaths } from "pages/routePaths";

type Entity = "source" | "destination" | "connection";

type Routes = {
  [key in Entity]: string;
};

const routes: Routes = {
  source: `../../${RoutePaths.Source}`,
  destination: `../../${RoutePaths.Destination}`,
  connection: `../../../${RoutePaths.Connections}`,
};

export function useDeleteModal(
  entity: Entity,
  onDelete: () => Promise<unknown>,
  additionalContent?: React.ReactNode,
  confirmationText?: string
) {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const navigate = useNavigate();

  return useCallback(() => {
    openConfirmationModal({
      text: `tables.${entity}DeleteModalText`,
      additionalContent,
      title: `tables.${entity}DeleteConfirm`,
      confirmationText,
      submitButtonText: "form.delete",
      onSubmit: async () => {
        await onDelete();
        closeConfirmationModal();
        navigate(routes[entity]);
      },
      submitButtonDataId: "delete",
    });
  }, [openConfirmationModal, entity, additionalContent, confirmationText, onDelete, closeConfirmationModal, navigate]);
}
