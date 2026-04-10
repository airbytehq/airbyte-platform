import { useCallback, useState } from "react";

import { useGetDataWorkerAvailability } from "core/api";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useFeature } from "core/services/features/FeatureService";
import { FeatureItem } from "core/services/features/types";
import { trackError } from "core/utils/datadog";

export const useSyncWithCapacityCheck = (syncConnection: () => Promise<void>) => {
  const isOnDemandCapacityEnabled = useFeature(FeatureItem.OnDemandCapacity);
  const getDataWorkerAvailability = useGetDataWorkerAvailability();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const [isCheckingCapacity, setIsCheckingCapacity] = useState(false);

  const syncWithCapacityCheck = useCallback(async () => {
    if (!isOnDemandCapacityEnabled) {
      return syncConnection();
    }

    setIsCheckingCapacity(true);
    try {
      const { outOfDataWorkers } = await getDataWorkerAvailability();

      if (!outOfDataWorkers) {
        return syncConnection();
      }

      openConfirmationModal({
        title: "connection.sync.capacityWarning.title",
        text: "connection.sync.capacityWarning.body",
        submitButtonText: "connection.sync.capacityWarning.submit",
        submitButtonVariant: "primary",
        onSubmit: async () => {
          await syncConnection();
          closeConfirmationModal();
        },
      });
    } catch (error) {
      trackError(error, { context: "data_worker_capacity_check" });
      return syncConnection();
    } finally {
      setIsCheckingCapacity(false);
    }
  }, [
    isOnDemandCapacityEnabled,
    syncConnection,
    getDataWorkerAvailability,
    openConfirmationModal,
    closeConfirmationModal,
  ]);

  return { syncWithCapacityCheck, isCheckingCapacity };
};
