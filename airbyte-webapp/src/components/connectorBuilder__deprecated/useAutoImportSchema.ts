import { useConnectorBuilderFormState } from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import { StreamId } from "./types";
import { useBuilderWatch } from "./useBuilderWatch";

// only auto import schema if it is enabled for the provided stream and connector is in draft mode
export const useAutoImportSchema = (streamId: StreamId) => {
  const { displayedVersion } = useConnectorBuilderFormState();
  const streams = useBuilderWatch("formValues.streams");
  const dynamicStreams = useBuilderWatch("formValues.dynamicStreams");

  if (displayedVersion !== undefined) {
    return false;
  }

  if (streamId.type === "generated_stream") {
    return dynamicStreams[dynamicStreams.findIndex((stream) => stream.dynamicStreamName === streamId.dynamicStreamName)]
      ?.streamTemplate.autoImportSchema;
  }

  if (streamId.type === "dynamic_stream") {
    return dynamicStreams[streamId.index]?.streamTemplate.autoImportSchema;
  }

  return streams[streamId.index]?.autoImportSchema;
};
