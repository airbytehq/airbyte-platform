import { dump } from "js-yaml";
import { FieldValues, UseFormGetValues } from "react-hook-form";

import { HttpRequest, HttpResponse } from "core/api/types/ConnectorBuilderClient";
import {
  ConditionalStreamsType,
  ConnectorManifest,
  DeclarativeComponentSchemaStreamsItem,
  DeclarativeStream,
  DeclarativeStreamType,
  DynamicDeclarativeStream,
  OAuthAuthenticatorType,
  SimpleRetrieverType,
  AsyncRetrieverType,
  HttpRequesterType,
  SimpleRetrieverRequester,
} from "core/api/types/ConnectorManifest";
import { BuilderView } from "services/connectorBuilder/ConnectorBuilderStateService";

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

export const getStreamName = (stream: DeclarativeComponentSchemaStreamsItem | undefined, index: number) => {
  const defaultName = `stream_${index}`;
  if (!stream || stream.type === ConditionalStreamsType.ConditionalStreams) {
    return defaultName;
  }
  return stream.name ?? defaultName;
};

const interpolatedConfigValueRegexBracket = /^\s*{{\s*config\[(?:'|")+(\S+)(?:'|")+\]\s*}}\s*$/;
const interpolatedConfigValueRegexDot = /^\s*{{\s*config\.(\S+)\s*}}\s*$/;

export function extractInterpolatedConfigPath(str: string): string;
export function extractInterpolatedConfigPath(str: string | undefined): string | undefined;
export function extractInterpolatedConfigPath(str: string | undefined): string | undefined {
  /**
   * This method works for nested configs like `config["credentials"]["client_secret"]`
   * and `config.credentials.client_secret` by parsing the full path and converting it to a dot notation key
   */
  if (str === undefined) {
    return undefined;
  }

  const regexBracketResult = interpolatedConfigValueRegexBracket.exec(str);
  if (regexBracketResult !== null) {
    if (regexBracketResult[1]) {
      return parseBracketNotation(regexBracketResult[1]);
    }
  }

  const regexDotResult = interpolatedConfigValueRegexDot.exec(str);
  if (regexDotResult !== null) {
    return regexDotResult[1];
  }

  return undefined;
}

const parseBracketNotation = (bracketPath: string): string => {
  // Remove quotes and split by '][' to handle nested brackets
  // Example: '"credentials"]["client_secret"' -> ['credentials', 'client_secret']
  const keys = bracketPath
    .split(/(?:'|")+\]\[(?:'|")+/)
    // remove outer quotes
    .map((key) => key.replace(/^['"]+|['"]+$/g, "").trim())
    .filter(Boolean);

  return keys.join(".");
};

export const getFirstOAuthStreamView = (getValues: UseFormGetValues<FieldValues>): BuilderView | undefined => {
  const streams = getValues("manifest.streams") as DeclarativeComponentSchemaStreamsItem[];
  const firstOAuthStreamIndex =
    streams?.findIndex(
      (stream) => stream.type === DeclarativeStreamType.DeclarativeStream && streamHasOAuthAuthenticator(stream)
    ) ?? -1;
  if (firstOAuthStreamIndex !== -1) {
    return {
      type: "stream",
      index: firstOAuthStreamIndex,
    };
  }

  const dynamicStreams = getValues("manifest.dynamic_streams") as DynamicDeclarativeStream[];
  const firstOAuthDynamicStreamIndex =
    dynamicStreams?.findIndex(
      (dynamicStream) =>
        dynamicStream.stream_template.type === DeclarativeStreamType.DeclarativeStream &&
        streamHasOAuthAuthenticator(dynamicStream.stream_template)
    ) ?? -1;
  if (firstOAuthDynamicStreamIndex !== -1) {
    return {
      type: "dynamic_stream",
      index: firstOAuthDynamicStreamIndex,
    };
  }

  return undefined;
};

const streamHasOAuthAuthenticator = (stream: DeclarativeStream): boolean => {
  const checkRequesterForAuthenticator = (requester: SimpleRetrieverRequester | undefined): boolean => {
    return (
      requester?.type === HttpRequesterType.HttpRequester &&
      requester.authenticator?.type === OAuthAuthenticatorType.OAuthAuthenticator
    );
  };

  // Check SimpleRetriever
  if (stream.retriever?.type === SimpleRetrieverType.SimpleRetriever) {
    return checkRequesterForAuthenticator(stream.retriever.requester);
  }

  // Check AsyncRetriever
  if (stream.retriever?.type === AsyncRetrieverType.AsyncRetriever) {
    return (
      checkRequesterForAuthenticator(stream.retriever.creation_requester) ||
      checkRequesterForAuthenticator(stream.retriever.polling_requester) ||
      checkRequesterForAuthenticator(stream.retriever.download_requester) ||
      checkRequesterForAuthenticator(stream.retriever.download_target_requester) ||
      checkRequesterForAuthenticator(stream.retriever.abort_requester) ||
      checkRequesterForAuthenticator(stream.retriever.delete_requester)
    );
  }

  // For other retriever types or if no retriever, return false
  return false;
};

interface TokenPathInfo {
  configPath: string;
  objectPath: string;
}

export const findOAuthTokenPaths = (
  manifestObject: Record<string, unknown>
): { accessTokenValues: TokenPathInfo[]; refreshTokens: TokenPathInfo[] } => {
  const accessTokenValues: TokenPathInfo[] = [];
  const refreshTokens: TokenPathInfo[] = [];

  const traverse = (obj: unknown, currentPath: string[] = []): void => {
    if (obj && typeof obj === "object") {
      const objectWithType = obj as Record<string, unknown>;

      // Check if current object is an OAuthAuthenticator
      if (objectWithType.type === OAuthAuthenticatorType.OAuthAuthenticator) {
        // Collect access_token_value if present
        if (objectWithType.access_token_value && typeof objectWithType.access_token_value === "string") {
          const configPath = extractInterpolatedConfigPath(objectWithType.access_token_value);
          if (configPath) {
            const objectPath = [...currentPath, "access_token_value"].join(".");
            accessTokenValues.push({ configPath, objectPath });
          }
        }

        // Collect refresh_token if present
        if (objectWithType.refresh_token && typeof objectWithType.refresh_token === "string") {
          const configPath = extractInterpolatedConfigPath(objectWithType.refresh_token);
          if (configPath) {
            const objectPath = [...currentPath, "refresh_token"].join(".");
            refreshTokens.push({ configPath, objectPath });
          }
        }
      }

      // Recursively traverse all properties, including arrays
      for (const key in objectWithType) {
        const value = objectWithType[key];
        if (Array.isArray(value)) {
          // Traverse each item in the array with index
          value.forEach((item, index) => traverse(item, [...currentPath, key, index.toString()]));
        } else if (typeof value === "object" && value !== null) {
          traverse(value, [...currentPath, key]);
        }
      }
    }
  };

  traverse(manifestObject);
  return { accessTokenValues, refreshTokens };
};
