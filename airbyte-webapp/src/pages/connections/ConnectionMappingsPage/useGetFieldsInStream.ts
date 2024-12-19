/**
 * todo: this lists all fields for a stream (maybe obvious) however, in reality we want all fields for a stream at the point of this mapping
 * that PR is in flight to be added at a (very soon) later time
 * https://github.com/airbytehq/airbyte-internal-issues/issues/11010
 */

import { useCurrentConnection } from "core/api";
import { traverseSchemaToField } from "core/domain/catalog";

import { getStreamDescriptorForKey } from "./MappingContext";

export const useGetFieldsInStream = (streamDescriptorKey: string) => {
  const { syncCatalog } = useCurrentConnection();
  const streamDescriptor = getStreamDescriptorForKey(streamDescriptorKey);

  if (!syncCatalog) {
    return [];
  }
  const stream = syncCatalog.streams.find(
    (s) => s.stream?.name === streamDescriptor.name && s.stream?.namespace === streamDescriptor.namespace
  );

  return traverseSchemaToField(stream?.stream?.jsonSchema, streamDescriptorKey).map((field) => ({
    fieldName: field.cleanedName,
    fieldType: field.type,
    airbyteType: field.airbyte_type,
  }));
};
