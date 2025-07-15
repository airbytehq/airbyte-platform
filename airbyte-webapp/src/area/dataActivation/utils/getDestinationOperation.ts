import { DestinationOperation, DestinationSyncMode } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";

export function getDestinationOperation(
  operations: DestinationOperation[],
  destinationObjectName: string,
  destinationSyncMode: DestinationSyncMode
) {
  const operation = operations.find(
    (operation) => operation.objectName === destinationObjectName && operation.syncMode === destinationSyncMode
  );

  if (!operation) {
    trackError(
      new Error(
        `No data activation operation found with object name ${destinationObjectName} and sync mode ${destinationSyncMode} and `
      ),
      { operations }
    );
  }
  return operation;
}
