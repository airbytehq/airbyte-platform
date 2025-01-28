import get from "lodash/get";
import isArray from "lodash/isArray";
import merge from "lodash/merge";
import { sha1 } from "object-hash";
import { useCallback } from "react";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { StreamReadTransformedSlices } from "core/api";
import { ConnectorBuilderProjectStreamReadSlicesItemPagesItemRecordsItem } from "core/api/types/AirbyteClient";
import { DeclarativeStream, PrimaryKey } from "core/api/types/ConnectorManifest";
import {
  convertJsonToYaml,
  useConnectorBuilderFormState,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderMetadata, StreamTestResults, useBuilderWatch } from "./types";
import { formatJson } from "./utils";

type StreamTestMetadataStatus = {
  isStale: boolean;
} & Omit<StreamTestResults, "streamHash">;

export interface TestWarning {
  message: string;
  priority: "primary" | "secondary";
}

export const getStreamHash = (resolvedStream: DeclarativeStream): string => {
  return sha1(formatJson(resolvedStream, true));
};

export const useStreamTestMetadata = () => {
  const { resolvedManifest, jsonManifest, updateJsonManifest } = useConnectorBuilderFormState();
  const { setValue } = useFormContext();
  const mode = useBuilderWatch("mode");
  const { formatMessage } = useIntl();

  const getStreamNameFromIndex = useCallback(
    (streamIndex: number) => {
      return resolvedManifest?.streams?.[streamIndex]?.name;
    },
    [resolvedManifest]
  );

  const updateStreamTestResults = useCallback(
    (
      streamRead: StreamReadTransformedSlices,
      resolvedTestStream: DeclarativeStream,
      streamName: string,
      streamIndex: number
    ) => {
      const streamTestResults = computeStreamTestResults(streamRead, resolvedTestStream);

      const newManifest = merge({}, jsonManifest, {
        metadata: { testedStreams: { [streamName]: streamTestResults } },
      });

      // If in UI mode, the form values are the source of truth defining the connector configuration, so the test results need to be set there.
      // If in YAML mode, the yaml value is the source of truth defining the connector configuration, so the test results need to be set there.
      // The underlying jsonManifest that gets saved to the DB and exported gets derived from either of the above depending on which mode is active.
      if (mode === "ui") {
        setValue(`formValues.streams.${streamIndex}.testResults`, streamTestResults);
      } else {
        setValue("yaml", convertJsonToYaml(newManifest));
      }

      // Update the jsonManifest with the new test results to avoid avoid lag between the setting the form values and the jsonManifest being updated,
      // which can cause an outdated warning to appear temporarily.
      updateJsonManifest(newManifest);
    },
    [jsonManifest, mode, setValue, updateJsonManifest]
  );

  const getStreamTestMetadataStatus = useCallback(
    (streamName: string): StreamTestMetadataStatus | undefined | null => {
      const resolvedStream = resolvedManifest?.streams?.find((stream) => stream.name === streamName);
      if (!resolvedStream) {
        // undefined indicates that the stream has not yet been resolved, so warnings should not be shown
        return undefined;
      }

      const metadata = jsonManifest.metadata as BuilderMetadata | undefined;
      if (
        !metadata ||
        !metadata.testedStreams ||
        !metadata.testedStreams[streamName] ||
        !metadata.testedStreams[streamName].streamHash ||
        metadata.testedStreams[streamName].streamHash === null
      ) {
        // null indicates that there is no test metadata for the stream, so it is untested
        return null;
      }

      const streamHash = getStreamHash(resolvedStream);

      const { streamHash: metadataStreamHash, ...testStatuses } = metadata.testedStreams[streamName];

      return {
        isStale: metadataStreamHash !== streamHash,
        ...testStatuses,
      };
    },
    [jsonManifest, resolvedManifest]
  );

  const getStreamTestWarnings = useCallback(
    (streamName: string): TestWarning[] => {
      const streamTestMetadataStatus = getStreamTestMetadataStatus(streamName);

      if (streamTestMetadataStatus === undefined) {
        return [];
      }

      if (streamTestMetadataStatus === null) {
        return [
          {
            message: formatMessage({ id: "connectorBuilder.warnings.untestedStream" }),
            priority: "primary",
          },
        ];
      }

      const warnings: TestWarning[] = [];
      const isStale = streamTestMetadataStatus.isStale;

      if (streamTestMetadataStatus.isStale) {
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
    [formatMessage, getStreamTestMetadataStatus]
  );

  return {
    getStreamNameFromIndex,
    updateStreamTestResults,
    getStreamTestMetadataStatus,
    getStreamTestWarnings,
  };
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
  resolvedTestStream: DeclarativeStream
): StreamTestResults => {
  const streamHash = getStreamHash(resolvedTestStream);

  if (streamRead.slices.length === 0 || streamRead.slices.every((slice) => slice.pages.length === 0)) {
    return {
      streamHash,
      hasResponse: false,
      responsesAreSuccessful: false,
      hasRecords: false,
      primaryKeysArePresent: false,
      primaryKeysAreUnique: false,
    };
  }

  return {
    streamHash,
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
  resolvedTestStream: DeclarativeStream
): Pick<StreamTestResults, "primaryKeysArePresent" | "primaryKeysAreUnique"> => {
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
