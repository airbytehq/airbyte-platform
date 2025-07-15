import { FieldValues, UseFormGetValues } from "react-hook-form";

import { HttpRequest, HttpResponse } from "core/api/types/ConnectorBuilderClient";
import {
  ConnectorManifest,
  DeclarativeComponentSchemaStreamsItem,
  DeclarativeStream,
  DeclarativeStreamType,
  OAuthAuthenticatorType,
  SimpleRetrieverType,
  HttpRequesterType,
  HttpRequester,
  OAuthAuthenticator,
  SimpleRetriever,
} from "core/api/types/ConnectorManifest";

import {
  convertJsonToYaml,
  extractInterpolatedConfigPath,
  formatForDisplay,
  formatJson,
  getStreamFieldPath,
  getStreamName,
  getFirstOAuthStreamView,
  findOAuthTokenPaths,
} from "./utils";

describe("connectorBuilder utils", () => {
  describe("formatJson", () => {
    it("formats JSON without ordering", () => {
      const obj = { b: 2, a: 1 };
      const result = formatJson(obj);
      expect(result).toBe('{\n  "b": 2,\n  "a": 1\n}');
    });

    it("formats JSON with ordering", () => {
      const obj = { b: 2, a: 1 };
      const result = formatJson(obj, true);
      expect(result).toBe('{\n  "a": 1,\n  "b": 2\n}');
    });

    it("formats complex nested objects with ordering", () => {
      const obj = { z: { y: 2, x: 1 }, a: 1 };
      const result = formatJson(obj, true);
      expect(result).toBe('{\n  "a": 1,\n  "z": {\n    "x": 1,\n    "y": 2\n  }\n}');
    });

    it("formats arrays correctly", () => {
      const obj = { items: [3, 1, 2] };
      const result = formatJson(obj);
      expect(result).toBe('{\n  "items": [\n    3,\n    1,\n    2\n  ]\n}');
    });

    it("handles null and undefined values", () => {
      const obj = { nullValue: null, undefinedValue: undefined };
      const result = formatJson(obj);
      expect(result).toBe('{\n  "nullValue": null\n}');
    });

    it("handles primitive values", () => {
      expect(formatJson("string")).toBe('"string"');
      expect(formatJson(123)).toBe("123");
      expect(formatJson(true)).toBe("true");
      expect(formatJson(null)).toBe("null");
    });
  });

  describe("formatForDisplay", () => {
    it("returns empty string for undefined input", () => {
      expect(formatForDisplay(undefined)).toBe("");
    });

    it("formats HttpRequest without body", () => {
      const request: HttpRequest = {
        url: "https://api.example.com",
        headers: { "Content-Type": "application/json" },
        http_method: "GET",
        body: "",
      };
      const result = formatForDisplay(request);
      expect(result).toContain('"url": "https://api.example.com"');
      expect(result).toContain('"body": ""');
    });

    it("formats HttpRequest with JSON body", () => {
      const request: HttpRequest = {
        url: "https://api.example.com",
        headers: { "Content-Type": "application/json" },
        http_method: "POST",
        body: '{"key": "value"}',
      };
      const result = formatForDisplay(request);
      expect(result).toContain('"body": {\n    "key": "value"\n  }');
    });

    it("formats HttpRequest with non-JSON body", () => {
      const request: HttpRequest = {
        url: "https://api.example.com",
        headers: { "Content-Type": "text/plain" },
        http_method: "POST",
        body: "plain text body",
      };
      const result = formatForDisplay(request);
      expect(result).toContain('"body": "plain text body"');
    });

    it("formats HttpResponse correctly", () => {
      const response: HttpResponse = {
        status: 200,
        headers: { "Content-Type": "application/json" },
        body: '{"success": true}',
      };
      const result = formatForDisplay(response);
      expect(result).toContain('"status": 200');
      expect(result).toContain('"body": {\n    "success": true\n  }');
    });

    it("handles invalid JSON in body gracefully", () => {
      const request: HttpRequest = {
        url: "https://api.example.com",
        headers: {},
        http_method: "POST",
        body: '{"invalid": json}',
      };
      const result = formatForDisplay(request);
      expect(result).toContain('"body": "{\\"invalid\\": json}"');
    });
  });

  describe("convertJsonToYaml", () => {
    it("converts simple manifest to YAML with correct ordering", () => {
      const manifest: ConnectorManifest = {
        version: "0.1.0",
        type: "DeclarativeSource",
        check: {
          type: "CheckStream",
          stream_names: ["test"],
        },
        streams: [],
      };

      const result = convertJsonToYaml(manifest);

      const expected = `version: 0.1.0

type: DeclarativeSource

check:
  type: CheckStream
  stream_names:
    - test

streams: []
`;

      expect(result).toBe(expected);
    });

    it("adds newlines between root-level fields", () => {
      const manifest: ConnectorManifest = {
        version: "0.1.0",
        type: "DeclarativeSource",
        check: {
          type: "CheckStream",
          stream_names: ["test"],
        },
        streams: [],
        spec: {
          type: "Spec",
          connection_specification: {
            type: "object",
            properties: {},
          },
        },
      };

      const result = convertJsonToYaml(manifest);

      const expected = `version: 0.1.0

type: DeclarativeSource

check:
  type: CheckStream
  stream_names:
    - test

streams: []

spec:
  type: Spec
  connection_specification:
    type: object
    properties: {}
`;

      expect(result).toBe(expected);
    });

    it("handles complex nested objects", () => {
      const manifest: ConnectorManifest = {
        version: "0.1.0",
        type: "DeclarativeSource",
        check: {
          type: "CheckStream",
          stream_names: ["test"],
        },
        streams: [],
        definitions: {
          base_requester: {
            type: "HttpRequester",
            url_base: "https://api.example.com",
          },
        },
      };

      const result = convertJsonToYaml(manifest);

      const expected = `version: 0.1.0

type: DeclarativeSource

check:
  type: CheckStream
  stream_names:
    - test

definitions:
  base_requester:
    type: HttpRequester
    url_base: https://api.example.com

streams: []
`;

      expect(result).toBe(expected);
    });
  });

  describe("getStreamFieldPath", () => {
    it("returns path for regular stream", () => {
      const streamId = { type: "stream" as const, index: 0 };
      const result = getStreamFieldPath(streamId);
      expect(result).toBe("manifest.streams.0");
    });

    it("returns path for regular stream with field path", () => {
      const streamId = { type: "stream" as const, index: 1 };
      const result = getStreamFieldPath(streamId, "name");
      expect(result).toBe("manifest.streams.1.name");
    });

    it("returns path for dynamic stream", () => {
      const streamId = { type: "dynamic_stream" as const, index: 0 };
      const result = getStreamFieldPath(streamId);
      expect(result).toBe("manifest.dynamic_streams.0");
    });

    it("returns template path for dynamic stream", () => {
      const streamId = { type: "dynamic_stream" as const, index: 0 };
      const result = getStreamFieldPath(streamId, "name", true);
      expect(result).toBe("manifest.dynamic_streams.0.stream_template.name");
    });

    it("returns path for generated stream", () => {
      const streamId = { type: "generated_stream" as const, dynamicStreamName: "users", index: 0 };
      const result = getStreamFieldPath(streamId);
      expect(result).toBe("generatedStreams.users.0");
    });

    it("returns path for generated stream with field path", () => {
      const streamId = { type: "generated_stream" as const, dynamicStreamName: "users", index: 1 };
      const result = getStreamFieldPath(streamId, "retriever.requester");
      expect(result).toBe("generatedStreams.users.1.retriever.requester");
    });
  });

  describe("getStreamName", () => {
    it("returns stream name for regular stream", () => {
      const stream: DeclarativeComponentSchemaStreamsItem = {
        type: "DeclarativeStream",
        name: "users",
      } as DeclarativeStream;

      const result = getStreamName(stream, 0);
      expect(result).toBe("users");
    });

    it("returns default name for stream without name", () => {
      const stream: DeclarativeComponentSchemaStreamsItem = {
        type: "DeclarativeStream",
      } as DeclarativeStream;

      const result = getStreamName(stream, 2);
      expect(result).toBe("stream_2");
    });

    it("returns default name for undefined stream", () => {
      const result = getStreamName(undefined, 3);
      expect(result).toBe("stream_3");
    });

    it("returns stream name even when empty (fallback logic is elsewhere)", () => {
      const stream: DeclarativeComponentSchemaStreamsItem = {
        type: "DeclarativeStream",
        name: "",
      } as DeclarativeStream;

      const result = getStreamName(stream, 0);
      expect(result).toBe("");
    });
  });

  describe("extractInterpolatedConfigPath", () => {
    it("extracts path from bracket notation", () => {
      const result = extractInterpolatedConfigPath('{{ config["api_key"] }}');
      expect(result).toBe("api_key");
    });

    it("extracts path from dot notation", () => {
      const result = extractInterpolatedConfigPath("{{ config.api_key }}");
      expect(result).toBe("api_key");
    });

    it("extracts nested path from bracket notation", () => {
      const result = extractInterpolatedConfigPath('{{ config["credentials"]["client_secret"] }}');
      expect(result).toBe("credentials.client_secret");
    });

    it("extracts nested path from dot notation", () => {
      const result = extractInterpolatedConfigPath("{{ config.credentials.client_secret }}");
      expect(result).toBe("credentials.client_secret");
    });

    it("handles single quotes in bracket notation", () => {
      const result = extractInterpolatedConfigPath("{{ config['api_key'] }}");
      expect(result).toBe("api_key");
    });

    it("handles mixed quotes in nested bracket notation", () => {
      const result = extractInterpolatedConfigPath(`{{ config['credentials']["client_secret"] }}`);
      expect(result).toBe("credentials.client_secret");
    });

    it("handles whitespace in expressions", () => {
      const result = extractInterpolatedConfigPath('{{  config["api_key"]  }}');
      expect(result).toBe("api_key");
    });

    it("returns undefined for non-matching strings", () => {
      expect(extractInterpolatedConfigPath("api_key")).toBeUndefined();
      expect(extractInterpolatedConfigPath("{{ some_other_var }}")).toBeUndefined();
      expect(extractInterpolatedConfigPath("{{ config }}")).toBeUndefined();
    });

    it("returns undefined for undefined input", () => {
      const result = extractInterpolatedConfigPath(undefined);
      expect(result).toBeUndefined();
    });

    it("returns undefined for empty string", () => {
      const result = extractInterpolatedConfigPath("");
      expect(result).toBeUndefined();
    });

    it("handles complex nested paths", () => {
      const result = extractInterpolatedConfigPath('{{ config["auth"]["credentials"]["oauth"]["client_id"] }}');
      expect(result).toBe("auth.credentials.oauth.client_id");
    });

    it("handles paths with special characters", () => {
      const result = extractInterpolatedConfigPath('{{ config["api-key_v2"] }}');
      expect(result).toBe("api-key_v2");
    });

    it("handles paths with numbers", () => {
      const result = extractInterpolatedConfigPath('{{ config["endpoint_v2"]["region1"] }}');
      expect(result).toBe("endpoint_v2.region1");
    });

    it("handles deeply nested dot notation", () => {
      const result = extractInterpolatedConfigPath("{{ config.auth.credentials.oauth.client_id }}");
      expect(result).toBe("auth.credentials.oauth.client_id");
    });
  });

  describe("getFirstOAuthStreamView", () => {
    it("returns undefined when no streams exist", () => {
      const mockGetValues = jest.fn((path: string) => {
        if (path === "manifest.streams") {
          return undefined;
        }
        if (path === "manifest.dynamic_streams") {
          return undefined;
        }
        return undefined;
      }) as unknown as UseFormGetValues<FieldValues>;

      const result = getFirstOAuthStreamView(mockGetValues);
      expect(result).toBeUndefined();
    });

    it("returns undefined when streams array is empty", () => {
      const mockGetValues = jest.fn((path: string) => {
        if (path === "manifest.streams") {
          return [];
        }
        if (path === "manifest.dynamic_streams") {
          return [];
        }
        return undefined;
      }) as unknown as UseFormGetValues<FieldValues>;

      const result = getFirstOAuthStreamView(mockGetValues);
      expect(result).toBeUndefined();
    });

    it("returns first OAuth stream view for regular streams", () => {
      const oauthAuthenticator: OAuthAuthenticator = {
        type: OAuthAuthenticatorType.OAuthAuthenticator,
        client_id: "test_client_id",
        client_secret: "test_client_secret",
        access_token_name: "access_token",
        token_refresh_endpoint: "https://auth.example.com/token",
        refresh_token_name: "refresh_token",
      };

      const httpRequester: HttpRequester = {
        type: HttpRequesterType.HttpRequester,
        url_base: "https://api.example.com",
        authenticator: oauthAuthenticator,
      };

      const simpleRetriever: SimpleRetriever = {
        type: SimpleRetrieverType.SimpleRetriever,
        requester: httpRequester,
        record_selector: {
          type: "RecordSelector",
          extractor: {
            type: "DpathExtractor",
            field_path: ["data"],
          },
        },
      };

      const oauthStream: DeclarativeStream = {
        type: DeclarativeStreamType.DeclarativeStream,
        name: "oauth_stream",
        retriever: simpleRetriever,
      };

      const regularStream: DeclarativeStream = {
        type: DeclarativeStreamType.DeclarativeStream,
        name: "regular_stream",
        retriever: {
          type: SimpleRetrieverType.SimpleRetriever,
          requester: {
            type: HttpRequesterType.HttpRequester,
            url_base: "https://api.example.com",
          },
          record_selector: {
            type: "RecordSelector",
            extractor: {
              type: "DpathExtractor",
              field_path: ["data"],
            },
          },
        },
      };

      const streams: DeclarativeComponentSchemaStreamsItem[] = [regularStream, oauthStream];

      const mockGetValues = jest.fn((path: string) => {
        if (path === "manifest.streams") {
          return streams;
        }
        if (path === "manifest.dynamic_streams") {
          return [];
        }
        return undefined;
      }) as unknown as UseFormGetValues<FieldValues>;

      const result = getFirstOAuthStreamView(mockGetValues);
      expect(result).toEqual({
        type: "stream",
        index: 1,
      });
    });

    it("returns undefined when no OAuth authenticators are found", () => {
      const regularStream: DeclarativeStream = {
        type: DeclarativeStreamType.DeclarativeStream,
        name: "regular_stream",
        retriever: {
          type: SimpleRetrieverType.SimpleRetriever,
          requester: {
            type: HttpRequesterType.HttpRequester,
            url_base: "https://api.example.com",
          },
          record_selector: {
            type: "RecordSelector",
            extractor: {
              type: "DpathExtractor",
              field_path: ["data"],
            },
          },
        },
      };

      const streams: DeclarativeComponentSchemaStreamsItem[] = [regularStream];

      const mockGetValues = jest.fn((path: string) => {
        if (path === "manifest.streams") {
          return streams;
        }
        if (path === "manifest.dynamic_streams") {
          return [];
        }
        return undefined;
      }) as unknown as UseFormGetValues<FieldValues>;

      const result = getFirstOAuthStreamView(mockGetValues);
      expect(result).toBeUndefined();
    });
  });

  describe("findOAuthTokenPaths", () => {
    it("returns empty arrays for empty manifest", () => {
      const manifest = {};
      const result = findOAuthTokenPaths(manifest);

      expect(result.accessTokenValues).toEqual([]);
      expect(result.refreshTokens).toEqual([]);
    });

    it("finds access token values in OAuth authenticators", () => {
      const manifest = {
        streams: [
          {
            retriever: {
              requester: {
                type: OAuthAuthenticatorType.OAuthAuthenticator,
                access_token_value: "{{ config.credentials.access_token }}",
              },
            },
          },
        ],
      };

      const result = findOAuthTokenPaths(manifest);

      expect(result.accessTokenValues).toEqual([
        {
          configPath: "credentials.access_token",
          objectPath: "streams.0.retriever.requester.access_token_value",
        },
      ]);
      expect(result.refreshTokens).toEqual([]);
    });

    it("finds refresh tokens in OAuth authenticators", () => {
      const manifest = {
        streams: [
          {
            retriever: {
              requester: {
                type: OAuthAuthenticatorType.OAuthAuthenticator,
                refresh_token: "{{ config.auth.refresh_token }}",
              },
            },
          },
        ],
      };

      const result = findOAuthTokenPaths(manifest);

      expect(result.accessTokenValues).toEqual([]);
      expect(result.refreshTokens).toEqual([
        {
          configPath: "auth.refresh_token",
          objectPath: "streams.0.retriever.requester.refresh_token",
        },
      ]);
    });

    it("finds both access token and refresh token in same authenticator", () => {
      const manifest = {
        streams: [
          {
            retriever: {
              requester: {
                type: OAuthAuthenticatorType.OAuthAuthenticator,
                access_token_value: "{{ config.credentials.access_token }}",
                refresh_token: "{{ config.credentials.refresh_token }}",
              },
            },
          },
        ],
      };

      const result = findOAuthTokenPaths(manifest);

      expect(result.accessTokenValues).toEqual([
        {
          configPath: "credentials.access_token",
          objectPath: "streams.0.retriever.requester.access_token_value",
        },
      ]);
      expect(result.refreshTokens).toEqual([
        {
          configPath: "credentials.refresh_token",
          objectPath: "streams.0.retriever.requester.refresh_token",
        },
      ]);
    });

    it("finds tokens in multiple OAuth authenticators", () => {
      const manifest = {
        streams: [
          {
            retriever: {
              requester: {
                type: OAuthAuthenticatorType.OAuthAuthenticator,
                access_token_value: "{{ config.auth1.access_token }}",
              },
            },
          },
          {
            retriever: {
              requester: {
                type: OAuthAuthenticatorType.OAuthAuthenticator,
                refresh_token: "{{ config.auth2.refresh_token }}",
              },
            },
          },
        ],
      };

      const result = findOAuthTokenPaths(manifest);

      expect(result.accessTokenValues).toEqual([
        {
          configPath: "auth1.access_token",
          objectPath: "streams.0.retriever.requester.access_token_value",
        },
      ]);
      expect(result.refreshTokens).toEqual([
        {
          configPath: "auth2.refresh_token",
          objectPath: "streams.1.retriever.requester.refresh_token",
        },
      ]);
    });

    it("ignores non-OAuth authenticators", () => {
      const manifest = {
        streams: [
          {
            retriever: {
              requester: {
                type: "ApiKeyAuthenticator",
                access_token_value: "{{ config.api_key }}",
              },
            },
          },
        ],
      };

      const result = findOAuthTokenPaths(manifest);

      expect(result.accessTokenValues).toEqual([]);
      expect(result.refreshTokens).toEqual([]);
    });

    it("ignores tokens that don't match interpolation pattern", () => {
      const manifest = {
        streams: [
          {
            retriever: {
              requester: {
                type: OAuthAuthenticatorType.OAuthAuthenticator,
                access_token_value: "static_token_value",
                refresh_token: "static_refresh_token",
              },
            },
          },
        ],
      };

      const result = findOAuthTokenPaths(manifest);

      expect(result.accessTokenValues).toEqual([]);
      expect(result.refreshTokens).toEqual([]);
    });

    it("handles deeply nested structures", () => {
      const manifest = {
        level1: {
          level2: {
            level3: [
              {
                authenticators: {
                  oauth: {
                    type: OAuthAuthenticatorType.OAuthAuthenticator,
                    access_token_value: "{{ config.deep.access_token }}",
                  },
                },
              },
            ],
          },
        },
      };

      const result = findOAuthTokenPaths(manifest);

      expect(result.accessTokenValues).toEqual([
        {
          configPath: "deep.access_token",
          objectPath: "level1.level2.level3.0.authenticators.oauth.access_token_value",
        },
      ]);
    });

    it("handles null and undefined values gracefully", () => {
      const manifest = {
        streams: [
          {
            retriever: {
              requester: {
                type: OAuthAuthenticatorType.OAuthAuthenticator,
                access_token_value: null,
                refresh_token: undefined,
              },
            },
          },
        ],
      };

      const result = findOAuthTokenPaths(manifest);

      expect(result.accessTokenValues).toEqual([]);
      expect(result.refreshTokens).toEqual([]);
    });
  });
});
