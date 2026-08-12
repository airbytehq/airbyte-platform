import { useCallback } from "react";

import { ProFeaturesWarnModal } from "area/organization/components/ProFeaturesWarnModal";
import { ORG_PLAN_IDS, useOrganizationPlan } from "area/organization/utils";
import { useModalService } from "core/services/Modal";

export const useProFeaturesModal = (featureId: string) => {
  const { isStandardPlan } = useOrganizationPlan();
  const { openModal } = useModalService();
  const currentStateKey = isStandardPlan ? ORG_PLAN_IDS.STANDARD : null;

  const showProFeatureModalIfNeeded = useCallback(async (): Promise<boolean> => {
    if (!currentStateKey) {
      return false;
    }

    await openModal({
      title: null,
      content: ({ onComplete }) => (
        <ProFeaturesWarnModal onContinue={() => onComplete("success")} featureId={featureId} />
      ),
      preventCancel: true,
      size: "xl",
    });

    return true;
  }, [featureId, currentStateKey, openModal]);

  return { showProFeatureModalIfNeeded };
};
