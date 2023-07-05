import { useMemo } from "react";

import { SyncSchemaStream } from "core/domain/catalog";

export const useStreamFilters = (
  searchString: string,
  hideDisabledStreams: boolean,
  sortedSchema: SyncSchemaStream[]
) => {
  return useMemo(() => {
    const filters: Array<(s: SyncSchemaStream) => boolean> = [
      (_: SyncSchemaStream) => true,
      searchString
        ? (stream: SyncSchemaStream) => stream.stream?.name.toLowerCase().includes(searchString.toLowerCase())
        : null,
      hideDisabledStreams ? (stream: SyncSchemaStream) => stream.config?.selected : null,
    ].filter(Boolean) as Array<(s: SyncSchemaStream) => boolean>;

    return sortedSchema.filter((stream) => filters.every((f) => f(stream)));
  }, [hideDisabledStreams, searchString, sortedSchema]);
};
