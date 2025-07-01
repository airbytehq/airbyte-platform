import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { StreamId } from "./types";
import { useBuilderWatch } from "./useBuilderWatch";
import { useStreamName } from "./useStreamNames";

// only auto import schema if it is enabled for the provided stream and connector is in draft mode
export const useAutoImportSchema = (streamId: StreamId) => {
  const { displayedVersion } = useConnectorBuilderFormState();

  const streamName = useStreamName(streamId);
  const autoImportSchema = useBuilderWatch(`manifest.metadata.autoImportSchema.${streamName}`) as boolean | undefined;

  if (displayedVersion !== "draft") {
    return false;
  }

  return autoImportSchema ?? false;
};
