import { AirbyteJsonSchema } from "components/forms/SchemaForm/utils";

import { ConnectorBuilderProjectTestingValues } from "core/api/types/AirbyteClient";
import {
  ApiKeyAuthenticatorType,
  BasicHttpAuthenticatorType,
  BearerAuthenticatorType,
  ConnectorManifest,
  DeclarativeStream,
  JwtAuthenticatorType,
  OAuthAuthenticatorType,
} from "core/api/types/ConnectorManifest";
import { AirbyteJSONSchema } from "core/jsonSchema/types";

import { INPUT_REFERENCE_KEYWORD, VALID_AUTHENTICATOR_TYPES } from "./constants";
import { AUTO_CREATED_INPUTS_BY_AUTH_TYPE } from "./useAuthenticatorInputs";
import declarativeComponentSchema from "../../../build/declarative_component_schema.yaml";

export interface GeneratedStreamId {
  type: "generated_stream";
  index: number;
  dynamicStreamName: string;
}

export interface StaticStreamId {
  type: "stream";
  index: number;
}

export interface DynamicStreamId {
  type: "dynamic_stream";
  index: number;
}

export type StreamId = StaticStreamId | GeneratedStreamId | DynamicStreamId;

export interface BuilderState {
  name: string;
  mode: "ui" | "yaml";
  yaml: string;
  customComponentsCode?: string;
  view: { type: "global" } | { type: "inputs" } | { type: "components" } | StreamId;
  streamTab: BuilderStreamTab;
  testStreamId: StreamId;
  testingValues: ConnectorBuilderProjectTestingValues | undefined;
  manifest: ConnectorManifest;
  generatedStreams: Record<string, DeclarativeStream[]>;
}

export type AuthenticatorType = (typeof VALID_AUTHENTICATOR_TYPES)[number];

const inputReference = (key: string | undefined) => (key ? `{{ ${INPUT_REFERENCE_KEYWORD}['${key}'] }}` : undefined);
const addAuthInputReferenceDefaults = (schema: AirbyteJsonSchema, authType: AuthenticatorType) => {
  const newProperties = {
    ...schema.properties,
  };

  const autoCreatedInputs = AUTO_CREATED_INPUTS_BY_AUTH_TYPE?.[authType] ?? {};
  for (const [key, value] of Object.entries(autoCreatedInputs)) {
    newProperties[key] = {
      ...(newProperties[key] as AirbyteJsonSchema),
      default: inputReference(value.key),
    };
  }

  return {
    ...schema,
    properties: newProperties,
  };
};

export const defaultBuilderStateSchema: AirbyteJsonSchema = {
  type: "object",
  properties: {
    name: {
      type: "string",
    },
    mode: {
      type: "string",
      enum: ["ui", "yaml"],
    },
    formValues: {
      type: "object",
    },
    previewValues: {
      type: "object",
    },
    yaml: {
      type: "string",
    },
    customComponentsCode: {
      type: "string",
    },
    view: {
      type: "object",
      oneOf: [
        {
          type: "object",
          properties: {
            type: { type: "string", enum: ["global"] },
          },
          required: ["type"],
        },
        {
          type: "object",
          properties: {
            type: { type: "string", enum: ["inputs"] },
          },
          required: ["type"],
        },
        {
          type: "object",
          properties: {
            type: { type: "string", enum: ["components"] },
          },
          required: ["type"],
        },
        {
          $ref: "#/definitions/StaticStreamId",
        },
        {
          $ref: "#/definitions/GeneratedStreamId",
        },
        {
          $ref: "#/definitions/DynamicStreamId",
        },
      ],
    },
    streamTab: {
      type: "string",
      enum: ["requester", "schema", "polling", "download"],
    },
    testStreamId: {
      type: "object",
      oneOf: [
        {
          $ref: "#/definitions/StaticStreamId",
        },
        {
          $ref: "#/definitions/GeneratedStreamId",
        },
        {
          $ref: "#/definitions/DynamicStreamId",
        },
      ],
    },
    testingValues: {
      type: "object",
      additionalProperties: true,
    },
    manifest: {
      type: "object",
      properties: {
        ...declarativeComponentSchema.properties,
      },
      required: declarativeComponentSchema.required,
    },
    generatedStreams: {
      type: "object",
      additionalProperties: {
        type: "array",
        items: { $ref: "#/definitions/DeclarativeStream" },
      },
    },
  },
  definitions: {
    ...declarativeComponentSchema.definitions,
    DeclarativeStream: {
      ...declarativeComponentSchema.definitions.DeclarativeStream,
      required: [...declarativeComponentSchema.definitions.DeclarativeStream.required, "name"],
    },
    StaticStreamId: {
      type: "object",
      properties: {
        type: { type: "string", enum: ["stream"] },
        index: { type: "number" },
      },
      required: ["type", "index"],
    },
    GeneratedStreamId: {
      type: "object",
      properties: {
        type: { type: "string", enum: ["generated_stream"] },
        index: { type: "number" },
        dynamicStreamName: { type: "string" },
      },
      required: ["type", "index", "dynamicStreamName"],
    },
    DynamicStreamId: {
      type: "object",
      properties: {
        type: { type: "string", enum: ["dynamic_stream"] },
        index: { type: "number" },
      },
      required: ["type", "index"],
    },
    // Add input reference default values for authenticator types
    ApiKeyAuthenticator: addAuthInputReferenceDefaults(
      declarativeComponentSchema.definitions.ApiKeyAuthenticator,
      ApiKeyAuthenticatorType.ApiKeyAuthenticator
    ),
    BasicHttpAuthenticator: addAuthInputReferenceDefaults(
      declarativeComponentSchema.definitions.BasicHttpAuthenticator,
      BasicHttpAuthenticatorType.BasicHttpAuthenticator
    ),
    BearerAuthenticator: addAuthInputReferenceDefaults(
      declarativeComponentSchema.definitions.BearerAuthenticator,
      BearerAuthenticatorType.BearerAuthenticator
    ),
    JwtAuthenticator: addAuthInputReferenceDefaults(
      declarativeComponentSchema.definitions.JwtAuthenticator,
      JwtAuthenticatorType.JwtAuthenticator
    ),
    OAuthAuthenticator: addAuthInputReferenceDefaults(
      declarativeComponentSchema.definitions.OAuthAuthenticator,
      OAuthAuthenticatorType.OAuthAuthenticator
    ),
  },
};

export interface AssistData {
  docsUrl?: string;
  openapiSpecUrl?: string;
}

export interface BuilderFormInput {
  key: string;
  required: boolean;
  definition: AirbyteJSONSchema;
  isLocked?: boolean;
}

export type BuilderStreamTab = "requester" | "schema" | "polling" | "download";

export interface StreamTestResults {
  streamHash: string | null;
  isStale?: boolean;
  hasResponse?: boolean;
  responsesAreSuccessful?: boolean;
  hasRecords?: boolean;
  primaryKeysArePresent?: boolean;
  primaryKeysAreUnique?: boolean;
}

export type GeneratedDeclarativeStream = DeclarativeStream & {
  dynamic_stream_name: string;
};

export function isStreamDynamicStream(streamId: StreamId): boolean {
  return streamId.type === "dynamic_stream";
}
