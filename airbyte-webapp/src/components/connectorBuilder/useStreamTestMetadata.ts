import get from "lodash/get";
import isArray from "lodash/isArray";
import merge from "lodash/merge";
import { sha1 } from "object-hash";
import { useCallback, useContext } from "react";
import { UseFormGetValues } from "react-hook-form";
import { useIntl } from "react-intl";

import { StreamReadTransformedSlices } from "core/api";
import { ConnectorBuilderProjectStreamReadSlicesItemPagesItemRecordsItem } from "core/api/types/AirbyteClient";
import {
  DeclarativeComponentSchemaStreamsItem,
  DeclarativeStream,
  DeclarativeStreamType,
  DynamicDeclarativeStream,
  PrimaryKey,
  StateDelegatingStream,
} from "core/api/types/ConnectorManifest";
import { ConnectorBuilderMainRHFContext } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderState, GeneratedStreamId, StaticStreamId, StreamId, StreamTestResults } from "./types";
import { formatJson } from "./utils";

type StreamTestMetadataStatus = {
  isStale: boolean;
} & Omit<StreamTestResults, "streamHash">;

export interface TestWarning {
  message: string;
  priority: "primary" | "secondary";
}

function useResolveStreamFromStreamId(getValues: UseFormGetValues<BuilderState>) {
  return (streamId: StaticStreamId | GeneratedStreamId) => {
    const resolvedStream =
      streamId.type === "generated_stream"
        ? getValues(`generatedStreams.${streamId.dynamicStreamName}.${streamId.index}`)
        : getValues(`manifest.streams.${streamId.index}`);

    return resolvedStream;
  };
}

export const getStreamHash = (resolvedStream: DeclarativeComponentSchemaStreamsItem): string => {
  return sha1(formatJson(resolvedStream, true));
};

export const useStreamTestMetadata = () => {
  const { watch, setValue, getValues } = useContext(ConnectorBuilderMainRHFContext) || {};
  if (!watch || !setValue || !getValues) {
    throw new Error("rhf context not available");
  }
  const { formatMessage } = useIntl();

  const testedStreams = watch("manifest.metadata.testedStreams");

  const getStreamNameFromIndex = useCallback(
    (streamIndex: number) => {
      return getValues(`manifest.streams.${streamIndex}.name`);
    },
    [getValues]
  );

  const resolveStreamFromStreamId = useResolveStreamFromStreamId(getValues);
  const updateStreamTestResults = useCallback(
    (streamRead: StreamReadTransformedSlices, resolvedTestStream: DeclarativeStream | StateDelegatingStream) => {
      const streamTestResults = computeStreamTestResults(streamRead, resolvedTestStream);

      const streamName = resolvedTestStream.name ?? "";

      setValue("manifest.metadata.testedStreams", merge({}, testedStreams, { [streamName]: streamTestResults }));
    },
    [setValue, testedStreams]
  );

  const getStreamTestMetadataStatus = useCallback(
    (streamId: StreamId): StreamTestMetadataStatus | undefined | null => {
      if (streamId.type === "dynamic_stream") {
        return undefined;
      }

      const streamName: string | undefined =
        streamId.type === "generated_stream"
          ? getValues(`generatedStreams.${streamId.dynamicStreamName}.${streamId.index}.name`)
          : getValues(`manifest.streams.${streamId.index}.name`);

      if (!streamName) {
        return undefined;
      }

      if (
        !testedStreams ||
        !testedStreams[streamName] ||
        !testedStreams[streamName].streamHash ||
        testedStreams[streamName].streamHash === null
      ) {
        // null indicates that there is no test metadata for the stream, so it is untested
        return null;
      }

      return testedStreams[streamName];
    },
    [getValues, testedStreams]
  );

  const getStreamTestWarnings = useCallback(
    (streamId: StreamId, ignoreStale: boolean = false): TestWarning[] => {
      if (streamId.type === "dynamic_stream") {
        const dynamicStream: DynamicDeclarativeStream | undefined = getValues(
          `manifest.dynamic_streams.${streamId.index}`
        );
        const thisGeneratedStreams: DeclarativeStream[] | undefined | object = getValues(
          `generatedStreams.${dynamicStream?.name ?? ""}`
        );

        // if the dynamic stream has no generated streams
        if (!thisGeneratedStreams || !Array.isArray(thisGeneratedStreams) || thisGeneratedStreams.length === 0) {
          return [
            {
              message: formatMessage({ id: "connectorBuilder.warnings.ungeneratedStreams" }),
              priority: "primary",
            },
          ];
        }

        // if all generated streams are untested
        if (
          thisGeneratedStreams.every(
            (_, index) =>
              getStreamTestMetadataStatus({
                type: "generated_stream",
                dynamicStreamName: dynamicStream?.name ?? "",
                index,
              }) === null
          )
        ) {
          return [
            {
              message: formatMessage({ id: "connectorBuilder.warnings.untestedDynamicStream" }),
              priority: "primary",
            },
          ];
        }

        const generatedStreamIds: GeneratedStreamId[] = thisGeneratedStreams.map((_stream, index) => {
          return {
            type: "generated_stream",
            dynamicStreamName: dynamicStream?.name ?? "",
            index,
          };
        });
        const generatedStreamWarnings = generatedStreamIds.flatMap((streamId) => {
          return getStreamTestWarnings(streamId, true);
        });
        if (generatedStreamWarnings.length > 0) {
          return [
            {
              message: formatMessage({ id: "connectorBuilder.warnings.generatedStreamWarnings" }),
              priority: "primary",
            },
          ];
        }
      }
      const streamTestMetadataStatus = getStreamTestMetadataStatus(streamId);

      if (streamTestMetadataStatus === undefined) {
        return [];
      }

      if (streamTestMetadataStatus === null) {
        if (ignoreStale) {
          return [];
        }
        return [
          {
            message: formatMessage({ id: "connectorBuilder.warnings.untestedStream" }),
            priority: "primary",
          },
        ];
      }

      const warnings: TestWarning[] = [];
      const isStale = streamTestMetadataStatus.isStale;

      if (!ignoreStale && streamTestMetadataStatus.isStale) {
        warnings.push({
          message: formatMessage({ id: "connectorBuilder.warnings.staleStreamTest" }),
          priority: "primary",
        });
      }

      if (isStatusFalse(streamTestMetadataStatus.hasResponse)) {
        warnings.push({
          message: formatMessage({ id: "connectorBuilder.warnings.noResponse" }),
          priority: isStale ? "secondary" : "primary",
        });
        return warnings;
      }

      if (isStatusFalse(streamTestMetadataStatus.responsesAreSuccessful)) {
        warnings.push({
          message: formatMessage({ id: "connectorBuilder.warnings.nonSuccessResponse" }),
          priority: isStale ? "secondary" : "primary",
        });
        return warnings;
      }

      if (isStatusFalse(streamTestMetadataStatus.hasRecords)) {
        warnings.push({
          message: formatMessage({ id: "connectorBuilder.warnings.noRecords" }),
          priority: isStale ? "secondary" : "primary",
        });
        return warnings;
      }

      if (isStatusFalse(streamTestMetadataStatus.primaryKeysArePresent)) {
        warnings.push({
          message: formatMessage({ id: "connectorBuilder.warnings.primaryKeyMissing" }),
          priority: isStale ? "secondary" : "primary",
        });
        return warnings;
      }

      if (isStatusFalse(streamTestMetadataStatus.primaryKeysAreUnique)) {
        warnings.push({
          message: formatMessage({ id: "connectorBuilder.warnings.primaryKeyDuplicate" }),
          priority: isStale ? "secondary" : "primary",
        });
        return warnings;
      }

      return warnings;
    },
    [formatMessage, getStreamTestMetadataStatus, getValues]
  );

  const getStreamHasCustomType = useCallback(
    (streamId: StreamId): boolean => {
      if (streamId.type === "dynamic_stream") {
        const dynamicStream: DynamicDeclarativeStream | undefined = getValues(
          `manifest.dynamic_streams.${streamId.index}`
        );
        if (!dynamicStream) {
          return false;
        }
        return hasCustomType(dynamicStream);
      }
      const stream = resolveStreamFromStreamId(streamId);
      return hasCustomType(stream);
    },
    [resolveStreamFromStreamId, getValues]
  );

  return {
    getStreamNameFromIndex,
    updateStreamTestResults,
    getStreamTestMetadataStatus,
    getStreamTestWarnings,
    getStreamHasCustomType,
  };
};

export const useSetStreamToStale = () => {
  const { setValue, getValues } = useContext(ConnectorBuilderMainRHFContext) || {};
  if (!setValue || !getValues) {
    throw new Error("rhf context not available");
  }
  return useCallback(
    (streamId: StreamId) => {
      if (streamId.type === "dynamic_stream" || streamId.type === "generated_stream") {
        return;
      }

      const streamName = getValues(`manifest.streams.${streamId.index}.name`);
      if (!streamName) {
        return;
      }

      const testMetadata = getValues(`manifest.metadata.testedStreams.${streamName}`);
      if (!testMetadata) {
        return;
      }

      if (testMetadata.isStale) {
        return;
      }

      setValue(`manifest.metadata.testedStreams.${streamName}.isStale`, true);
    },
    [getValues, setValue]
  );
};

// Explicitly check if status is false in the rest of the test statuses, as
// an explicit false value means that the stream was tested in the Builder and
// failed that test, whereas an undefined values means that the stream comes
// from outside of the Builder, for which we assume it is already tested
const isStatusFalse = (status: boolean | undefined): boolean => {
  return status === false;
};

const computeStreamTestResults = (
  streamRead: StreamReadTransformedSlices,
  resolvedTestStream: DeclarativeComponentSchemaStreamsItem
): StreamTestResults => {
  const streamHash = getStreamHash(resolvedTestStream);

  if (streamRead.slices.length === 0 || streamRead.slices.every((slice) => slice.pages.length === 0)) {
    return {
      streamHash,
      isStale: false,
      hasResponse: false,
      responsesAreSuccessful: false,
      hasRecords: false,
      primaryKeysArePresent: false,
      primaryKeysAreUnique: false,
    };
  }

  return {
    streamHash,
    isStale: false,
    hasResponse: true,
    responsesAreSuccessful: streamRead.slices.every((slice) =>
      slice.pages.every((page) => !page.response || (page.response.status >= 200 && page.response.status < 300))
    ),
    hasRecords: streamRead.slices.some((slice) => slice.pages.some((page) => page.records.length > 0)),
    ...computePrimaryKeyTestResults(streamRead, resolvedTestStream),
  };
};

const computePrimaryKeyTestResults = (
  streamRead: StreamReadTransformedSlices,
  resolvedTestStream: DeclarativeComponentSchemaStreamsItem
): Pick<StreamTestResults, "primaryKeysArePresent" | "primaryKeysAreUnique"> => {
  if (resolvedTestStream.type !== DeclarativeStreamType.DeclarativeStream) {
    return {
      primaryKeysArePresent: true,
      primaryKeysAreUnique: true,
    };
  }

  const normalizedPrimaryKey = normalizePrimaryKey(resolvedTestStream.primary_key);
  if (normalizedPrimaryKey === undefined) {
    return {
      primaryKeysArePresent: true,
      primaryKeysAreUnique: true,
    };
  }

  const primaryKeyValueSet = new Set();
  for (const slice of streamRead.slices) {
    for (const page of slice.pages) {
      for (const record of page.records) {
        const primaryKeyValue = getPrimaryKeyValue(record, normalizedPrimaryKey);
        if (primaryKeyValue === undefined) {
          return {
            primaryKeysArePresent: false,
            primaryKeysAreUnique: false,
          };
        }

        if (primaryKeyValueSet.has(primaryKeyValue)) {
          return {
            primaryKeysArePresent: true,
            primaryKeysAreUnique: false,
          };
        }
        primaryKeyValueSet.add(primaryKeyValue);
      }
    }
  }

  return {
    primaryKeysArePresent: true,
    primaryKeysAreUnique: true,
  };
};

const getPrimaryKeyValue = (
  record: ConnectorBuilderProjectStreamReadSlicesItemPagesItemRecordsItem,
  primaryKey: string[][]
) => {
  const values = primaryKey.map((keyPart) => {
    const value = get(record, keyPart);
    return value ? JSON.stringify(value) : undefined;
  });

  if (values.filter((value) => value !== undefined).length === 0) {
    return undefined;
  }

  return values.map((value) => JSON.stringify(value)).join(", ");
};

const normalizePrimaryKey = (primaryKey: PrimaryKey | undefined): string[][] | undefined => {
  if (primaryKey === undefined) {
    return undefined;
  }

  if (!isArray(primaryKey)) {
    return [[primaryKey]];
  }

  if (isDoublyNestedArray(primaryKey)) {
    return primaryKey;
  }

  return primaryKey.map((keyPart) => [keyPart]);
};

const isDoublyNestedArray = <T>(value: T[] | T[][]): value is T[][] => {
  return isArray(value[0]);
};

// Recursively scans `obj` to extract any string "type" values.
const collectTypes = (obj: unknown) => {
  const types: string[] = [];

  if (typeof obj === "object" && obj != null) {
    if ("type" in obj && typeof obj.type === "string") {
      types.push(obj.type);
    }

    Object.values(obj).forEach((value) => {
      types.push(...collectTypes(value));
    });
  }

  return types;
};

// Checks if `obj` has any "type" values that start with "Custom".
const hasCustomType = (obj: unknown) => {
  const types = new Set<string>(collectTypes(obj));
  for (const type of types) {
    if (type.match(/^Custom[A-Z]/)) {
      return true;
    }
  }
  return false;
};
