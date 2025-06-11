import { dump } from "js-yaml";
import { SetValueConfig, useFormContext } from "react-hook-form";

import { HttpRequest, HttpResponse } from "core/api/types/ConnectorBuilderClient";
import {
  ConnectorManifest,
  DeclarativeStream,
  SimpleRetrieverPartitionRouter,
  SimpleRetrieverPartitionRouterAnyOfItem,
} from "core/api/types/ConnectorManifest";

import { StreamId } from "./types";

export function formatJson(json: unknown, order?: boolean): string {
  return JSON.stringify(order ? orderKeys(json) : json, null, 2);
}

export function formatForDisplay(obj: HttpRequest | HttpResponse | undefined): string {
  if (!obj) {
    return "";
  }

  let parsedBody: unknown;
  try {
    // body is a string containing JSON most of the time, but not always.
    // Attempt to parse and fall back to the raw string if unsuccessful.
    parsedBody = obj.body ? JSON.parse(obj.body) : "";
  } catch {
    parsedBody = obj.body;
  }

  const unpackedObj = {
    ...obj,
    body: parsedBody,
  };
  return formatJson(unpackedObj);
}

function orderKeys(obj: unknown): unknown {
  if (Array.isArray(obj)) {
    return obj.map(orderKeys);
  }
  if (typeof obj !== "object" || obj === null) {
    return obj;
  }
  return Object.fromEntries(
    Object.entries(obj)
      .sort(([key1], [key2]) => {
        if (key1 < key2) {
          return -1;
        }
        if (key1 > key2) {
          return 1;
        }

        // names must be equal
        return 0;
      })
      .map(([key, val]) => [key, orderKeys(val)])
  );
}

export type StreamFieldPath = `formValues.streams.${number}.${string}`;

export const useCopyValueIncludingArrays = () => {
  const { getValues, setValue, control } = useFormContext();

  const replaceStreamIndex = (path: string, streamIndex: number) => {
    return path.replace(/^formValues.streams.\d+/, `formValues.streams.${streamIndex}`);
  };

  return (fromStream: number, toStream: number, streamFieldPath: StreamFieldPath, setValueOptions: SetValueConfig) => {
    const fromPath = replaceStreamIndex(streamFieldPath, fromStream);
    const toPath = replaceStreamIndex(streamFieldPath, toStream);
    const valueToCopy = getValues(fromPath);
    setValue(toPath, valueToCopy, setValueOptions);

    // must explicitly call setValue on each array's path, so that useFieldArray properly reacts to it
    const affectedArrayPaths = [...control._names.array].filter((arrayPath) => arrayPath.includes(toPath));

    affectedArrayPaths.forEach((arrayPath) => {
      const arrayValueToCopy = getValues(replaceStreamIndex(arrayPath, fromStream));
      setValue(replaceStreamIndex(arrayPath, toStream), arrayValueToCopy, setValueOptions);
    });
  };
};

export function filterPartitionRouterToType(
  partitionRouter: SimpleRetrieverPartitionRouter | undefined,
  types: Array<SimpleRetrieverPartitionRouterAnyOfItem["type"]>
) {
  if (!partitionRouter) {
    return undefined;
  }

  if (Array.isArray(partitionRouter)) {
    return partitionRouter.filter((subRouter) => subRouter.type && types.includes(subRouter.type));
  }

  if (types.includes(partitionRouter.type)) {
    return [partitionRouter];
  }

  return undefined;
}

export function streamRef(streamName: string) {
  // force cast to DeclarativeStream so that this still validates against the types
  return { $ref: `#/definitions/streams/${streamName}` } as unknown as DeclarativeStream;
}

export function streamNameOrDefault(streamName: string | undefined, index: number) {
  return streamName || `stream_${index}`;
}

const MANIFEST_KEY_ORDER: Array<keyof ConnectorManifest> = [
  "version",
  "type",
  "description",
  "check",
  "definitions",
  "streams",
  "spec",
  "metadata",
  "schemas",
];
export function convertJsonToYaml(json: ConnectorManifest): string {
  const yamlString = dump(json, {
    noRefs: true,
    quotingType: '"',
    sortKeys: (a: keyof ConnectorManifest, b: keyof ConnectorManifest) => {
      const orderA = MANIFEST_KEY_ORDER.indexOf(a);
      const orderB = MANIFEST_KEY_ORDER.indexOf(b);
      if (orderA === -1 && orderB === -1) {
        return 0;
      }
      if (orderA === -1) {
        return 1;
      }
      if (orderB === -1) {
        return -1;
      }
      return orderA - orderB;
    },
  });

  // add newlines between root-level fields
  return yamlString.replace(/^\S+.*/gm, (match, offset) => {
    return offset > 0 ? `\n${match}` : match;
  });
}

export const getStreamFieldPath = <T extends string>(streamId: StreamId, fieldPath?: T, templatePath?: boolean) => {
  const basePath =
    streamId.type === "stream"
      ? `manifest.streams.${streamId.index}`
      : streamId.type === "dynamic_stream"
      ? templatePath
        ? `manifest.dynamic_streams.${streamId.index}.stream_template`
        : `manifest.dynamic_streams.${streamId.index}`
      : `generatedStreams.${streamId.dynamicStreamName}.${streamId.index}`;

  if (fieldPath) {
    return `${basePath}.${fieldPath}`;
  }
  return basePath;
};
