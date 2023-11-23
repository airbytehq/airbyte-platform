import React, { useCallback } from "react";
import { useNavigate } from "react-router-dom";

import { RoutePaths } from "pages/routePaths";

import { useConfirmationModalService } from "./services/ConfirmationModal";

type Entity = "source" | "destination" | "connection";

type Routes = {
  [key in Entity]: string;
};

const routes: Routes = {
  source: `../../${RoutePaths.Source}`,
  destination: `../../${RoutePaths.Destination}`,
  connection: `../../../${RoutePaths.Connections}`,
};

export function useDeleteModal(entity: Entity, onDelete: () => Promise<unknown>, additionalContent?: React.ReactNode) {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const navigate = useNavigate();

  return useCallback(() => {
    openConfirmationModal({
      text: `tables.${entity}DeleteModalText`,
      additionalContent,
      title: `tables.${entity}DeleteConfirm`,
      submitButtonText: "form.delete",
      onSubmit: async () => {
        await onDelete();
        closeConfirmationModal();
        navigate(routes[entity]);
      },
      submitButtonDataId: "delete",
    });
  }, [openConfirmationModal, entity, additionalContent, onDelete, closeConfirmationModal, navigate]);
}
