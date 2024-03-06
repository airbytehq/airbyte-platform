import { useMemo } from "react";

import { AirbyteStreamAndConfiguration } from "core/api/types/AirbyteClient";

import { SyncStreamFieldWithId } from "../../ConnectionForm/formConfig";

export const useStreamFilters = (
  searchString: string,
  hideDisabledStreams: boolean,
  sortedSchema: SyncStreamFieldWithId[]
): SyncStreamFieldWithId[] => {
  return useMemo(() => {
    const filters: Array<(s: AirbyteStreamAndConfiguration) => boolean> = [
      searchString
        ? (stream: AirbyteStreamAndConfiguration) =>
            stream.stream?.name.toLowerCase().includes(searchString.toLowerCase())
        : null,
      hideDisabledStreams ? (stream: AirbyteStreamAndConfiguration) => stream.config?.selected : null,
    ].filter(Boolean) as Array<(s: AirbyteStreamAndConfiguration) => boolean>;

    return sortedSchema.filter((stream) => filters.every((f) => f(stream)));
  }, [hideDisabledStreams, searchString, sortedSchema]);
};
