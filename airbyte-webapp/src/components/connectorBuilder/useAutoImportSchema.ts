import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { StreamId } from "./types";
import { useBuilderWatch } from "./useBuilderWatch";

// only auto import schema if it is enabled for the provided stream and connector is in draft mode
export const useAutoImportSchema = (streamId: StreamId) => {
  const { displayedVersion } = useConnectorBuilderFormState();
  const streams = useBuilderWatch("formValues.streams");

  if (streamId.type === "dynamic_stream") {
    return false;
  }
  return streams[streamId.index]?.autoImportSchema && displayedVersion === undefined;
};
