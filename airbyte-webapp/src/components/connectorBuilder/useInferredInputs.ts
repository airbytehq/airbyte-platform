import { useMemo } from "react";

import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { getInferredInputList, hasIncrementalSyncUserInput } from "./types";

export const useInferredInputs = () => {
  const {
    builderFormValues: { global, inferredInputOverrides, streams },
  } = useConnectorBuilderFormState();
  const startDateInput = hasIncrementalSyncUserInput(streams, "start_datetime");
  const endDateInput = hasIncrementalSyncUserInput(streams, "end_datetime");
  return useMemo(
    () => getInferredInputList(global, inferredInputOverrides, startDateInput, endDateInput),
    [endDateInput, global, inferredInputOverrides, startDateInput]
  );
};
