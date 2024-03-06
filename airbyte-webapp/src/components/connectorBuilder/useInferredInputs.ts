import { useContext } from "react";

import { ConnectorBuilderMainRHFContext } from "services/connectorBuilder/ConnectorBuilderStateService";

import { getInferredInputList, hasIncrementalSyncUserInput } from "./types";

export const useInferredInputs = () => {
  const { watch } = useContext(ConnectorBuilderMainRHFContext) || {};
  if (!watch) {
    throw new Error("rhf context not available");
  }
  const authenticator = watch("formValues.global.authenticator");
  const inferredInputOverrides = watch("formValues.inferredInputOverrides");
  const streams = watch("formValues.streams");
  const startDateInput = hasIncrementalSyncUserInput(streams, "start_datetime");
  const endDateInput = hasIncrementalSyncUserInput(streams, "end_datetime");
  return getInferredInputList(authenticator, inferredInputOverrides, startDateInput, endDateInput);
};
