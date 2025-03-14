import { SetValueConfig, useFormContext } from "react-hook-form";

import { HttpRequest, HttpResponse } from "core/api/types/ConnectorBuilderClient";
import {
  DeclarativeStream,
  SimpleRetrieverPartitionRouter,
  SimpleRetrieverPartitionRouterAnyOfItem,
} from "core/api/types/ConnectorManifest";

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
