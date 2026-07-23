import cloneDeep from "lodash/cloneDeep";
import { useEffect, useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";

import { StreamId } from "./types";
import { useBuilderWatch } from "./useBuilderWatch";
import { getStreamFieldPath } from "./utils";

// When a stream is renamed, the metadata that is tied to its name needs to be updated
// to point to the new name, which this hook does.
export const useUpdateMetadata = (streamId: StreamId) => {
  const { setValue } = useFormContext();
  const metadata = useWatch({ name: "manifest.metadata" });
  const streamName = useBuilderWatch(getStreamFieldPath(streamId, "name")) as string | undefined;
  const [prevName, setPrevName] = useState(streamName);

  useEffect(() => {
    if (!streamName || streamId.type === "generated_stream") {
      return;
    }
    if (!prevName) {
      const metadataClone = cloneDeep(metadata);
      const newMetadata = {
        ...metadataClone,
        autoImportSchema: {
          ...metadataClone?.autoImportSchema,
          [streamName]: true,
        },
      };
      setValue("manifest.metadata", newMetadata);
    }
    if (metadata && prevName && streamName !== prevName) {
      const newMetadata = cloneDeep(metadata);
      if (metadata.autoImportSchema && metadata.autoImportSchema[prevName] !== undefined) {
        delete newMetadata.autoImportSchema[prevName];
        newMetadata.autoImportSchema[streamName] = metadata.autoImportSchema[prevName];
      }
      if (metadata.testedStreams && metadata.testedStreams[prevName] !== undefined) {
        delete newMetadata.testedStreams[prevName];
        newMetadata.testedStreams[streamName] = metadata.testedStreams[prevName];
      }
      setValue("manifest.metadata", newMetadata);
    }
    setPrevName(streamName);
  }, [streamName, prevName, metadata, setValue, streamId.type]);
};
