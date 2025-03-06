import { useCurrentConnection, useInitialValidation } from "core/api";
import { traverseSchemaToField } from "core/domain/catalog";

import { getStreamDescriptorForKey, useMappingContext } from "./MappingContext";

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
export const useGetFieldsForMapping = (streamDescriptorKey: string, mapperId: string) => {
  const { streamsWithMappings } = useMappingContext();
  const mappings = streamsWithMappings[streamDescriptorKey];
  const initialData = useInitialValidation(getStreamDescriptorForKey(streamDescriptorKey), mappings);

  if (!initialData) {
    return [];
  }

  const mapperIdx = initialData?.mappers.findIndex((m) => m.id === mapperId);

  if (mapperIdx === undefined || mapperIdx === -1) {
    return [];
  }

  if (mapperIdx === 0) {
    return initialData?.initialFields.sort((a, b) => {
      return a.name.localeCompare(b.name);
    });
  }

  return initialData?.mappers[mapperIdx - 1].outputFields.sort((a, b) => {
    return a.name.localeCompare(b.name);
  });
};
