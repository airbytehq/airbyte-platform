import { useMemo } from "react";
import { useWatch } from "react-hook-form";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { DestinationCatalog } from "core/api/types/AirbyteClient";

export function useSelectedDestinationOperation(destinationCatalog: DestinationCatalog, streamIndex: number) {
  const destinationSyncMode = useWatch<DataActivationConnectionFormValues, `streams.${number}.destinationSyncMode`>({
    name: `streams.${streamIndex}.destinationSyncMode`,
  });
  const destinationObjectName = useWatch<DataActivationConnectionFormValues, `streams.${number}.destinationObjectName`>(
    {
      name: `streams.${streamIndex}.destinationObjectName`,
    }
  );
  return useMemo(() => {
    return destinationCatalog.operations.find(
      (operation) => operation.syncMode === destinationSyncMode && operation.objectName === destinationObjectName
    );
  }, [destinationCatalog, destinationObjectName, destinationSyncMode]);
}
