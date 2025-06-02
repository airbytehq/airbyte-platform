import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { StreamId, getStreamFieldPath } from "./types";
import { useBuilderWatch } from "./useBuilderWatch";

// only auto import schema if it is enabled for the provided stream and connector is in draft mode
export const useAutoImportSchema = (streamId: StreamId) => {
  const { displayedVersion } = useConnectorBuilderFormState();

  const namePath = getStreamFieldPath(streamId, "name");
  const streamName = useBuilderWatch(namePath) as string | undefined;
  const autoImportSchema = useBuilderWatch(`manifest.metadata.autoImportSchema.${streamName}`) as boolean | undefined;

  if (displayedVersion !== undefined) {
    return false;
  }

  return autoImportSchema;
};
