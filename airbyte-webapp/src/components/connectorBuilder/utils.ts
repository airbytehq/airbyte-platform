import get from "lodash/get";
import isArray from "lodash/isArray";
import isObjectLike from "lodash/isObjectLike";
import isString from "lodash/isString";
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

export const useCopyValueIncludingArrays = () => {
  const { getValues, setValue, control } = useFormContext();

  const streamPath = (streamNum: number, pathInStream: string) => `formValues.streams.${streamNum}.${pathInStream}`;

  return (fromStream: number, toStream: number, pathInStream: string, setValueOptions: SetValueConfig) => {
    const valueToCopy = getValues(streamPath(fromStream, pathInStream));
    setValue(streamPath(toStream, pathInStream), valueToCopy, setValueOptions);

    // must explicitly call setValue on each array's path, so that useFieldArray properly reacts to it
    const affectedArrayPathsInStream = [...control._names.array]
      .filter((arrayPath) => arrayPath.includes(streamPath(toStream, pathInStream)))
      .map((fullArrayPath) => {
        const regex = /streams\.\d+\.(.*)/;
        const match = fullArrayPath.match(regex);
        if (match && match.length >= 2) {
          return match[1];
        }
        return undefined;
      })
      .filter((arrayPathInStream?: string): arrayPathInStream is string => Boolean(arrayPathInStream));

    affectedArrayPathsInStream.forEach((arrayPathInStream) => {
      const arrayValueToCopy = getValues(streamPath(fromStream, arrayPathInStream));
      setValue(streamPath(toStream, arrayPathInStream), arrayValueToCopy, setValueOptions);
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

type RecordObject = Record<string, unknown>;
const isRecordObject = (value: unknown): value is RecordObject => {
  return isObjectLike(value) && !isArray(value);
};

export class ManifestValidationError extends Error {
  constructor(
    public errors: string[],
    // nestedErrors holds errors for oneOf values. This is separated as these errors are
    // not always relevant to the manifest, as they contain validation errors for every
    // option in the oneOf, not just the one that the user intends to use
    public nestedErrors?: string[]
  ) {
    super({ errors, nestedErrors }.toString());
  }
}

/**
 * Resolves a $ref value by retrieving value at that path in `obj`.
 *
 * Only supports local refs referring to other parts of the same object.
 *
 * Throws an error in each of the following cases
 * - `refValue` is not of the form `#/path/to/value`
 * - there is no value at that path in `obj`
 * - the value at that path is not an object or is an array
 */
const resolveRef = (refValue: string, obj: RecordObject): RecordObject => {
  // matches values of the form `#/path/to/value`, pulling `path/to/value` out as a match group
  const match = refValue.match(/^#\/(.+)$/);
  if (!match) {
    throw new ManifestValidationError([
      `invalid $ref value: ${refValue} — $refs must be of the form '#/path/to/value'`,
    ]);
  }

  // replace all `/` with `.`, since lodash get() expects dot-separated paths
  const refValueJsonPath = match[1].replaceAll("/", ".");
  const value = get(obj, refValueJsonPath);
  if (value === undefined) {
    throw new ManifestValidationError([`$ref value ${refValue} could not be found`]);
  }
  if (!isRecordObject(value)) {
    throw new ManifestValidationError([`$ref value ${refValue} does not point to an object`]);
  }

  return value;
};

/**
 * Recursively finds all $refs in the `current` argument, and replaces them with the
 * keys and values of the object they refer to, retrieving that object from the
 * `rootObj` using the path in the $ref's value.
 *
 * The `rootObj` is always passed through to the next recursive call unchanged, so
 * that the path that any $ref refers to can be retrieved.
 *
 * The `visitedRefPaths` set is used to detect circular references, and is updated
 * with the $ref value each time a $ref is recursively resolved.
 *
 * Throws a ManifestValidationError if the $ref value is not a string or if it is
 * part of a circular reference.
 *
 * Returns the `current` with all nested $refs resolved.
 *
 * @param current
 * @param rootObj
 * @returns
 */
const recursivelyResolveRefs = <T>(current: T, rootObj: RecordObject, visitedRefPaths: Set<string>): T => {
  if (!isRecordObject(current)) {
    if (isArray(current)) {
      return current.map((item) => recursivelyResolveRefs(item, rootObj, visitedRefPaths)) as T;
    }

    return current;
  }

  let result: T = {
    ...current,
  };

  const { $ref, ...restCurrent } = current;
  if ($ref) {
    if (!isString($ref)) {
      throw new ManifestValidationError([
        `invalid $ref value: ${$ref} — $refs must be a string of the form '#/path/to/value'`,
      ]);
    }

    if (visitedRefPaths.has($ref)) {
      throw new ManifestValidationError([`circular reference detected: ${$ref}`]);
    }

    const resolvedRef = resolveRef($ref, rootObj);
    const updatedVisitedRefPaths = new Set(visitedRefPaths).add($ref);

    // resolve any nested $refs in the resolved ref value
    const recursivelyResolvedRef = recursivelyResolveRefs(resolvedRef, rootObj, updatedVisitedRefPaths);

    result = {
      ...recursivelyResolvedRef,
      ...restCurrent,
    } as T;
  }

  for (const key in result) {
    result[key] = recursivelyResolveRefs(result[key], rootObj, visitedRefPaths);
  }

  return result;
};

/**
 * Finds any `$ref` keys in the provided object, and replaces them with the keys and
 * values of the object they they refer to.
 *
 * $refs come from the JSON Schema spec — more info can be found here:
 * https://json-schema.org/understanding-json-schema/structuring#dollarref
 *
 * While that spec only defines $refs in the context of a schema, we also use $refs in
 * manifest JSON objects to refer to other parts of the manifest, to cut down on
 * duplication and make the manifest easier to read.
 *
 * However, there are some places where a fully resolved object is needed, such as
 * when calculating the hash of a child stream — in this case we want the parent stream
 * to be included in the hash, not just the reference to it, so that if the parent
 * stream changes, the hash changes as well.
 *
 * This function helps with that by resolving all $refs in an object.
 */
export const resolveRefs = async <T>(obj: T): Promise<T> => {
  if (!isRecordObject(obj)) {
    return obj;
  }
  return recursivelyResolveRefs(obj, obj, new Set());
};
