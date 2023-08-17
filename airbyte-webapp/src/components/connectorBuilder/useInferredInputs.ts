import { useContext, useMemo } from "react";

import { ConnectorBuilderMainRHFContext } from "services/connectorBuilder/ConnectorBuilderStateService";

import { getInferredInputList, hasIncrementalSyncUserInput } from "./types";

export const useInferredInputs = () => {
  const { watch } = useContext(ConnectorBuilderMainRHFContext) || {};
  if (!watch) {
    throw new Error("rhf context not available");
  }
  const global = watch("formValues.global");
  const inferredInputOverrides = watch("formValues.inferredInputOverrides");
  const streams = watch("formValues.streams");
  const startDateInput = hasIncrementalSyncUserInput(streams, "start_datetime");
  const endDateInput = hasIncrementalSyncUserInput(streams, "end_datetime");
  return useMemo(
    () => getInferredInputList(global, inferredInputOverrides, startDateInput, endDateInput),
    [endDateInput, global, inferredInputOverrides, startDateInput]
  );
};
