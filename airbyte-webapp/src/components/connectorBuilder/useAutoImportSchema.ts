import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { StreamId } from "./types";
import { useBuilderWatch } from "./useBuilderWatch";
import { getStreamFieldPath } from "./utils";

// only auto import schema if it is enabled for the provided stream and connector is in draft mode
export const useAutoImportSchema = (streamId: StreamId) => {
  const { displayedVersion } = useConnectorBuilderFormState();

  const namePath = getStreamFieldPath(streamId, "name");
  const streamName = useBuilderWatch(namePath) as string | undefined;
  const autoImportSchema = useBuilderWatch(`manifest.metadata.autoImportSchema.${streamName}`) as boolean | undefined;

  if (displayedVersion !== "draft") {
    return false;
  }

  return autoImportSchema;
};
