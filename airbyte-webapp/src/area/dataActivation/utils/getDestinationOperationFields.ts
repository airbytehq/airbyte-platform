import { DestinationOperation } from "core/api/types/AirbyteClient";

export const getDestinationOperationFields = (selectedOperation: DestinationOperation | undefined) => {
  const topLevelFields = selectedOperation?.schema?.properties ?? [];
  return Object.entries(topLevelFields);
};
