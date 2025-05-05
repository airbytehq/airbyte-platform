import { dump } from "js-yaml";
import merge from "lodash/merge";

import {
  AddedFieldDefinitionType,
  ConnectorManifest,
  DeclarativeStream,
  DeclarativeStreamIncrementalSync,
  HttpRequesterErrorHandler,
  RecordSelector,
  SessionTokenAuthenticator,
  SimpleRetrieverPaginator,
  Spec,
} from "core/api/types/ConnectorManifest";

import {
  manifestAuthenticatorToBuilder,
  manifestErrorHandlerToBuilder,
  manifestIncrementalSyncToBuilder,
  manifestPaginatorToBuilder,
  manifestRecordSelectorToBuilder,
  manifestSubstreamPartitionRouterToBuilder,
  manifestTransformationsToBuilder,
} from "./convertManifestToBuilderForm";
import {
  DEFAULT_BUILDER_FORM_VALUES,
  DEFAULT_CONNECTOR_NAME,
  OLDEST_SUPPORTED_CDK_VERSION,
  isYamlString,
} from "./types";
import { convertToBuilderFormValues } from "./useManifestToBuilderForm";
import { formatJson } from "./utils";

jest.mock("services/connectorBuilder/ConnectorBuilderStateService", () => ({}));

const baseManifest: ConnectorManifest = {
  type: "DeclarativeSource",
  version: OLDEST_SUPPORTED_CDK_VERSION,
  check: {
    type: "CheckStream",
    stream_names: [],
    dynamic_streams_check_configs: [],
  },
  streams: [],
};

const apiTokenAuthenticator = {
  type: "ApiKeyAuthenticator" as const,
  api_token: "{{ config['api_token'] }}",
  header: "API_KEY",
};

const apiTokenSpec: Spec = {
  type: "Spec",
  connection_specification: {
    type: "object",
    required: ["api_token"],
    properties: {
      api_token: {
        type: "string",
        title: "API Token",
        airbyte_secret: true,
      },
    },
  },
};

const oauthAuthenticator = {
  type: "OAuthAuthenticator" as const,
  client_id: "{{ config['client_id'] }}",
  client_secret: "{{ config['client_secret'] }}",
  refresh_token: "{{ config['client_refresh_token'] }}",
  refresh_request_body: {
    key1: "val1",
    key2: "val2",
  },
  token_refresh_endpoint: "https://api.com/refresh_token",
  grant_type: "refresh_token",
};

const oauthSpec: Spec = {
  type: "Spec",
  connection_specification: {
    type: "object",
    required: ["client_id", "client_secret", "client_refresh_token"],
    properties: {
      client_id: {
        type: "string",
        title: "Client ID",
        airbyte_secret: true,
      },
      client_secret: {
        type: "string",
        title: "Client Secret",
        airbyte_secret: true,
      },
      client_refresh_token: {
        type: "string",
        title: "Client Refresh Token",
        airbyte_secret: true,
      },
      oauth_access_token: {
        type: "string",
        title: "Access Token",
        airbyte_secret: true,
      },
      oauth_token_expiry_date: {
        type: "string",
        title: "Token Expiry Date",
      },
    },
  },
};

const stream1: DeclarativeStream = {
  type: "DeclarativeStream",
  name: "stream1",
  retriever: {
    type: "SimpleRetriever",
    record_selector: {
      type: "RecordSelector",
      extractor: {
        type: "DpathExtractor",
        field_path: [],
      },
    },
    requester: {
      type: "HttpRequester",
      url_base: "https://url.com",
      path: "/stream-1-path",
    },
  },
};

const stream2: DeclarativeStream = merge({}, stream1, {
  name: "stream2",
  retriever: {
    name: "stream2",
    requester: {
      name: "stream2",
      path: "/stream-2-path",
    },
  },
});

const noOpResolve = (manifest: ConnectorManifest) => {
  return Promise.resolve({ manifest });
};

describe("Conversion throws error when", () => {
  it("resolve throws error", async () => {
    const errorMessage = "Some resolve error message";
    const resolve = (_manifest: ConnectorManifest) => {
      throw new Error(errorMessage);
    };
    const convert = async () => {
      return convertToBuilderFormValues(resolve, {} as ConnectorManifest, DEFAULT_CONNECTOR_NAME);
    };
    await expect(convert).rejects.toThrow(errorMessage);
  });

  it("manifests has incorrect retriever type", async () => {
    const convert = async () => {
      const manifest: ConnectorManifest = {
        ...baseManifest,
        streams: [
          {
            type: "DeclarativeStream",
            name: "firstStream",
            retriever: {
              type: "CustomRetriever",
              class_name: "some_python_class",
            },
          },
        ],
      };
      return convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    };
    await expect(convert).rejects.toThrow("doesn't use a AsyncRetriever");
  });

  it("manifest has incorrect requester type", async () => {
    const convert = async () => {
      const manifest: ConnectorManifest = {
        ...baseManifest,
        streams: [
          merge({}, stream1, {
            retriever: {
              requester: {
                type: "CustomRequester",
                class_name: "some_python_class",
              },
            },
          }),
        ],
      };
      return convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    };
    await expect(convert).rejects.toThrow("doesn't use a HttpRequester");
  });

  it("manifest has an authenticator with a non-interpolated secret key", async () => {
    const convert = () => {
      const authenticator = {
        type: "ApiKeyAuthenticator" as const,
        api_token: "abcd1234",
        header: "API_KEY",
      };
      return manifestAuthenticatorToBuilder(authenticator, undefined, undefined);
    };
    expect(convert).toThrow('ApiKeyAuthenticator.api_token must be of the form {{ config["key"] }}');
  });

  it("manifest has an authenticator with an interpolated key that doesn't match any spec key", async () => {
    const convert = () => manifestAuthenticatorToBuilder(apiTokenAuthenticator, undefined, undefined);
    expect(convert).toThrow(
      'ApiKeyAuthenticator.api_token references spec key "api_token", which must appear in the spec'
    );
  });

  it("manifest has an authenticator with a required interpolated key that is not required in the spec", async () => {
    const convert = () =>
      manifestAuthenticatorToBuilder(apiTokenAuthenticator, undefined, {
        type: "Spec",
        connection_specification: {
          type: "object",
          properties: {
            api_token: {
              type: "string",
              title: "API Token",
              airbyte_secret: true,
            },
          },
        },
      });
    expect(convert).toThrow(
      'ApiKeyAuthenticator.api_token references spec key "api_token", which must be required in the spec'
    );
  });

  it("manifest has an authenticator with an interpolated key that is not type string in the spec", async () => {
    const convert = () =>
      manifestAuthenticatorToBuilder(apiTokenAuthenticator, undefined, {
        type: "Spec",
        connection_specification: {
          type: "object",
          required: ["api_token"],
          properties: {
            api_token: {
              type: "integer",
              title: "API Token",
              airbyte_secret: true,
            },
          },
        },
      });
    expect(convert).toThrow(
      'ApiKeyAuthenticator.api_token references spec key "api_token", which must be of type string'
    );
  });

  it("manifest has an authenticator with an interpolated secret key that is not secret in the spec", async () => {
    const convert = () =>
      manifestAuthenticatorToBuilder(apiTokenAuthenticator, undefined, {
        type: "Spec",
        connection_specification: {
          type: "object",
          required: ["api_token"],
          properties: {
            api_token: {
              type: "string",
              title: "API Token",
            },
          },
        },
      });
    expect(convert).toThrow(
      'ApiKeyAuthenticator.api_token references spec key "api_token", which must have airbyte_secret set to true'
    );
  });

  it("manifest has an OAuthAuthenticator with a refresh_request_body containing non-string values", async () => {
    const convert = () => {
      const authenticator = {
        type: "OAuthAuthenticator" as const,
        client_id: "{{ config['client_id'] }}",
        client_secret: "{{ config['client_secret'] }}",
        refresh_token: "{{ config['client_refresh_token'] }}",
        refresh_request_body: {
          key1: "val1",
          key2: {
            a: 1,
            b: 2,
          },
        },
        token_refresh_endpoint: "https://api.com/refresh_token",
        grant_type: "client_credentials",
      };
      return manifestAuthenticatorToBuilder(authenticator, undefined, oauthSpec);
    };
    expect(convert).toThrow("OAuthAuthenticator contains a refresh_request_body with non-string values");
  });

  it("manifest has a SessionTokenAuthenticator with an unsupported login_requester authenticator type", async () => {
    const convert = () => {
      const authenticator: SessionTokenAuthenticator = {
        type: "SessionTokenAuthenticator",
        login_requester: {
          type: "HttpRequester",
          url_base: "https://api.com/",
          path: "/session",
          authenticator: oauthAuthenticator,
          http_method: "POST",
          request_parameters: {},
          request_headers: {},
          request_body_json: {},
        },
        session_token_path: ["id"],
        expiration_duration: "P2W",
        request_authentication: {
          type: "ApiKey",
          inject_into: {
            type: "RequestOption",
            field_name: "X-Session-Token",
            inject_into: "header",
          },
        },
      };
      return manifestAuthenticatorToBuilder(authenticator, undefined, undefined);
    };
    expect(convert).toThrow(
      "SessionTokenAuthenticator.login_requester.authenticator must have one of the following types: NoAuth, ApiKeyAuthenticator, BearerAuthenticator, BasicHttpAuthenticator"
    );
  });

  it("manifest has a paginator with an unsupported type", async () => {
    const convert = () => {
      const paginator = {
        type: "UnsupportedPaginatorHandler",
      };
      return manifestPaginatorToBuilder(paginator as SimpleRetrieverPaginator);
    };
    expect(convert).toThrow("doesn't use a DefaultPaginator");
  });

  it("manifest has an error handler with an unsupported type", async () => {
    const convert = () => {
      const errorHandler = {
        type: "UnsupportedErrorHandler",
      };
      return manifestErrorHandlerToBuilder(errorHandler as HttpRequesterErrorHandler);
    };
    expect(convert).toThrow("error handler type 'UnsupportedErrorHandler' is unsupported");
  });

  it("manifest has an incremental sync with an unsupported type", async () => {
    const convert = () => {
      const incrementalSync = {
        type: "UnsupportedIncrementalSync",
      };
      return manifestIncrementalSyncToBuilder(incrementalSync as DeclarativeStreamIncrementalSync);
    };
    expect(convert).toThrow("doesn't use a DatetimeBasedCursor");
  });

  it("manifest has a record selector with an unsupported type", async () => {
    const convert = () => {
      const recordSelector = {
        type: "UnsupportedRecordSelector",
      };
      return manifestRecordSelectorToBuilder(recordSelector as RecordSelector);
    };
    expect(convert).toThrow("doesn't use a RecordSelector");
  });

  it("manifest has a record extractor with an unsupported type", async () => {
    const convert = () => {
      const recordSelector = {
        type: "RecordSelector",
        extractor: {
          type: "UnsupportedExtractor",
        },
      };
      return manifestRecordSelectorToBuilder(recordSelector as RecordSelector);
    };
    expect(convert).toThrow("doesn't use a DpathExtractor");
  });

  it("manifest has a record filter with an unsupported type", async () => {
    const convert = () => {
      const recordSelector = {
        type: "RecordSelector",
        extractor: {
          type: "DpathExtractor",
          field_path: [],
        },
        record_filter: {
          type: "UnsupportedRecordFilter",
        },
      };
      return manifestRecordSelectorToBuilder(recordSelector as RecordSelector);
    };
    expect(convert).toThrow("doesn't use a RecordFilter");
  });

  it("SubstreamPartitionRouter doesn't use a $ref for the parent stream", async () => {
    const convert = () => {
      const partitionRouter = {
        type: "SubstreamPartitionRouter" as const,
        parent_stream_configs: [
          {
            type: "ParentStreamConfig" as const,
            parent_key: "id",
            partition_field: "parent_id",
            stream: stream2,
          },
        ],
      };
      return manifestSubstreamPartitionRouterToBuilder(partitionRouter, { stream2: "stream2" });
    };
    expect(convert).toThrow("SubstreamPartitionRouter.parent_stream_configs.stream must use $ref");
  });

  it("SubstreamPartitionRouter's parent stream $ref does not point to a stream definition", async () => {
    const convert = () => {
      const partitionRouter = {
        type: "SubstreamPartitionRouter" as const,
        parent_stream_configs: [
          {
            type: "ParentStreamConfig" as const,
            parent_key: "id",
            partition_field: "parent_id",
            stream: {
              $ref: "#/definitions/notAStream",
            } as unknown as DeclarativeStream,
          },
        ],
      };
      return manifestSubstreamPartitionRouterToBuilder(partitionRouter, { stream2: "stream2" });
    };
    expect(convert).toThrow(
      "SubstreamPartitionRouter's parent stream reference must match the pattern '#/definitions/streams/streamName'"
    );
  });

  it("SubstreamPartitionRouter's parent stream $ref does not match any stream name", async () => {
    const convert = () => {
      const partitionRouter = {
        type: "SubstreamPartitionRouter" as const,
        parent_stream_configs: [
          {
            type: "ParentStreamConfig" as const,
            parent_key: "id",
            partition_field: "parent_id",
            stream: {
              $ref: "#/definitions/streams/stream1",
            } as unknown as DeclarativeStream,
          },
        ],
      };
      return manifestSubstreamPartitionRouterToBuilder(partitionRouter, { stream2: "stream2" });
    };
    expect(convert).toThrow(
      "SubstreamPartitionRouter references parent stream name 'stream1' which could not be found"
    );
  });

  it("unknown fields are found on component", async () => {
    const convert = () => {
      const transformations = [
        {
          type: "AddFields" as const,
          fields: [
            {
              type: AddedFieldDefinitionType.AddedFieldDefinition,
              path: ["path", "to", "field"],
              value: "my_value",
            },
          ],
          unsupported_field: "abc",
        },
      ];
      return manifestTransformationsToBuilder(transformations);
    };
    expect(convert).toThrow("AddFields contains fields unsupported by the UI: unsupported_field");
  });
});

describe("Conversion successfully results in", () => {
  it("default values if manifest is empty", async () => {
    const formValues = await convertToBuilderFormValues(noOpResolve, baseManifest, DEFAULT_CONNECTOR_NAME);
    expect(formValues).toEqual(DEFAULT_BUILDER_FORM_VALUES);
  });

  it("spec properties converted to inputs if no streams present", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      spec: {
        type: "Spec",
        connection_specification: {
          $schema: "http://json-schema.org/draft-07/schema#",
          type: "object",
          required: ["api_key"],
          properties: {
            api_key: {
              type: "string",
              title: "API Key",
              airbyte_secret: true,
            },
          },
        },
      },
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    expect(formValues.inputs).toEqual([
      {
        key: "api_key",
        required: true,
        isLocked: false,
        // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
        definition: manifest.spec?.connection_specification.properties.api_key,
      },
    ]);
  });

  it("spec conversion not failing on no required property", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      spec: {
        type: "Spec",
        connection_specification: {
          $schema: "http://json-schema.org/draft-07/schema#",
          type: "object",
          properties: {
            api_key: {
              type: "string",
              title: "API Key",
              airbyte_secret: true,
            },
          },
        },
      },
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    expect(formValues.inputs).toEqual([
      {
        key: "api_key",
        required: false,
        isLocked: false,
        // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
        definition: manifest.spec?.connection_specification.properties.api_key,
      },
    ]);
  });

  it("spec properties converted to locked inputs on matching auth keys", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            requester: {
              authenticator: {
                type: "ApiKeyAuthenticator",
                api_token: "{{ config['api_key'] }}",
                header: "API_KEY",
              },
            },
          },
        }),
      ],
      spec: {
        type: "Spec",
        connection_specification: {
          $schema: "http://json-schema.org/draft-07/schema#",
          type: "object",
          required: ["api_key"],
          properties: {
            api_key: {
              type: "string",
              title: "API Key",
              airbyte_secret: true,
            },
            numeric_key: {
              type: "number",
              title: "Numeric key",
            },
          },
        },
      },
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    expect(formValues.inputs).toEqual([
      {
        key: "api_key",
        required: true,
        isLocked: true,
        // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
        definition: manifest.spec?.connection_specification.properties.api_key,
      },
      {
        key: "numeric_key",
        required: false,
        isLocked: false,
        // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
        definition: manifest.spec?.connection_specification.properties.numeric_key,
      },
    ]);
  });

  it("request options converted to key-value list", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            requester: {
              request_parameters: {
                k1: "v1",
                k2: "v2",
              },
            },
          },
        }),
      ],
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    if (formValues.streams[0].requestType === "async") {
      throw new Error("Request type should not be async");
    }
    expect(formValues.streams[0].requestOptions.requestParameters).toEqual([
      ["k1", "v1"],
      ["k2", "v2"],
    ]);
  });

  it("request json body converted to key-value list", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            requester: {
              request_body_json: {
                k1: "v1",
                k2: "v2",
              },
            },
          },
        }),
      ],
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    if (formValues.streams[0].requestType === "async") {
      throw new Error("Request type should not be async");
    }
    expect(formValues.streams[0].requestOptions.requestBody).toEqual({
      type: "json_list",
      values: [
        ["k1", "v1"],
        ["k2", "v2"],
      ],
    });
  });

  it("nested request json body converted to string", async () => {
    const body = {
      k1: { nested: "v1" },
      k2: "v2",
    };
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            requester: {
              request_body_json: body,
            },
          },
        }),
      ],
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    if (formValues.streams[0].requestType === "async") {
      throw new Error("Request type should not be async");
    }
    expect(formValues.streams[0].requestOptions.requestBody).toEqual({
      type: "json_freeform",
      value: formatJson(body),
    });
  });

  it("request data body converted to list", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            requester: {
              request_body_data: {
                k1: "v1",
                k2: "v2",
              },
            },
          },
        }),
      ],
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    if (formValues.streams[0].requestType === "async") {
      throw new Error("Request type should not be async");
    }
    expect(formValues.streams[0].requestOptions.requestBody).toEqual({
      type: "form_list",
      values: [
        ["k1", "v1"],
        ["k2", "v2"],
      ],
    });
  });

  it("record selector converted to builder record selector", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            record_selector: {
              type: "RecordSelector",
              extractor: {
                type: "DpathExtractor",
                field_path: ["a", "b"],
              },
              record_filter: {
                type: "RecordFilter",
                condition: "{{ record.c > 1 }}",
              },
              schema_normalization: "Default",
            },
          },
        }),
      ],
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    if (formValues.streams[0].requestType === "async") {
      throw new Error("Request type should not be async");
    }
    expect(formValues.streams[0].recordSelector).toEqual({
      fieldPath: ["a", "b"],
      filterCondition: "{{ record.c > 1 }}",
      normalizeToSchema: true,
    });
  });

  it("string body converted to string", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            requester: {
              request_body_data: "abc def",
            },
          },
        }),
      ],
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    if (formValues.streams[0].requestType === "async") {
      throw new Error("Request type should not be async");
    }
    expect(formValues.streams[0].requestOptions.requestBody).toEqual({
      type: "string_freeform",
      value: "abc def",
    });
  });

  it("primary key string converted to array", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          primary_key: "id",
        }),
      ],
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    if (formValues.streams[0].requestType === "async") {
      throw new Error("Request type should not be async");
    }
    expect(formValues.streams[0].primaryKey).toEqual(["id"]);
  });

  it("multi list partition router converted to builder parameterized requests", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            partition_router: [
              {
                type: "ListPartitionRouter",
                cursor_field: "id",
                values: ["slice1", "slice2"],
              },
              {
                type: "ListPartitionRouter",
                cursor_field: "id2",
                values: "{{ config['abc'] }}",
              },
            ],
          },
        }),
      ],
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    if (formValues.streams[0].requestType === "async") {
      throw new Error("Request type should not be async");
    }
    expect(formValues.streams[0].parameterizedRequests).toEqual([
      {
        type: "ListPartitionRouter",
        cursor_field: "id",
        values: { type: "list", value: ["slice1", "slice2"] },
      },
      {
        type: "ListPartitionRouter",
        cursor_field: "id2",
        values: { type: "variable", value: "{{ config['abc'] }}" },
      },
    ]);
  });

  it("substream partition router converted to builder parent streams", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        stream1,
        merge({}, stream2, {
          retriever: {
            partition_router: {
              type: "SubstreamPartitionRouter",
              parent_stream_configs: [
                {
                  type: "ParentStreamConfig",
                  parent_key: "key",
                  stream: stream1,
                  partition_field: "field",
                },
              ],
            },
          },
        }),
      ],
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    if (formValues.streams[1].requestType === "async") {
      throw new Error("Request type should not be async");
    }
    expect(formValues.streams[1].parentStreams).toEqual([
      {
        parent_key: "key",
        partition_field: "field",
        parentStreamReference: "0",
      },
    ]);
  });

  it("schema loader converted to schema with ordered keys in JSON", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          schema_loader: {
            type: "InlineSchemaLoader",
            schema: {
              b: "yyy",
              a: "xxx",
            },
          },
        }),
      ],
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    expect(formValues.streams[0].schema).toEqual(
      `{
  "a": "xxx",
  "b": "yyy"
}`
    );
  });

  it('authenticator with a interpolated secret key of type config.key converted to config["key"]', async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            requester: {
              authenticator: {
                type: "ApiKeyAuthenticator",
                api_token: "{{ config.api_token }}",
                header: "API_KEY",
              },
            },
          },
        }),
      ],
      spec: apiTokenSpec,
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    if (isYamlString(formValues.global.authenticator)) {
      throw new Error("Authenticator should not be a YAML string");
    }
    if (formValues.global.authenticator.type !== "ApiKeyAuthenticator") {
      throw new Error("Authenticator should have type ApiKeyAuthenticator");
    }
    expect(formValues.global.authenticator.api_token).toEqual('{{ config["api_token"] }}');
  });

  it("OAuth authenticator refresh_request_body converted to array", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            requester: {
              authenticator: oauthAuthenticator,
            },
          },
        }),
      ],
      spec: oauthSpec,
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    expect(formValues.global.authenticator).toEqual({
      type: "OAuthAuthenticator",
      client_id: '{{ config["client_id"] }}',
      client_secret: '{{ config["client_secret"] }}',
      refresh_token: '{{ config["client_refresh_token"] }}',
      refresh_request_body: [
        ["key1", "val1"],
        ["key2", "val2"],
      ],
      token_refresh_endpoint: "https://api.com/refresh_token",
      grant_type: "refresh_token",
    });
  });

  it("OAuthAuthenticator with refresh token updater is converted", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            requester: {
              authenticator: {
                type: "OAuthAuthenticator",
                client_id: "{{ config['client_id'] }}",
                client_secret: "{{ config['client_secret'] }}",
                refresh_token: "{{ config['client_refresh_token'] }}",
                token_refresh_endpoint: "https://api.com/refresh_token",
                grant_type: "refresh_token",
                refresh_token_updater: {
                  refresh_token_name: "refresh_token",
                  access_token_config_path: ["oauth_access_token"],
                  refresh_token_config_path: ["client_refresh_token"],
                  token_expiry_date_config_path: ["oauth_token_expiry_date"],
                },
              },
            },
          },
        }),
      ],
      spec: oauthSpec,
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    expect(formValues.global.authenticator).toEqual({
      type: "OAuthAuthenticator",
      client_id: '{{ config["client_id"] }}',
      client_secret: '{{ config["client_secret"] }}',
      refresh_token: '{{ config["client_refresh_token"] }}',
      refresh_request_body: [],
      token_refresh_endpoint: "https://api.com/refresh_token",
      grant_type: "refresh_token",
      refresh_token_updater: {
        refresh_token_name: "refresh_token",
        access_token: '{{ config["oauth_access_token"] }}',
        token_expiry_date: '{{ config["oauth_token_expiry_date"] }}',
      },
    });
  });

  it("SessionTokenAuthenticator with types properly converted to builder form types", async () => {
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [
        merge({}, stream1, {
          retriever: {
            requester: {
              authenticator: {
                type: "SessionTokenAuthenticator",
                login_requester: {
                  type: "HttpRequester",
                  url_base: "https://api.com/",
                  path: "/session",
                  authenticator: {
                    type: "NoAuth",
                  },
                  error_handler: {
                    type: "CompositeErrorHandler",
                    error_handlers: [
                      {
                        type: "DefaultErrorHandler",
                        backoff_strategies: [
                          {
                            type: "ConstantBackoffStrategy",
                            backoff_time_in_seconds: 5,
                          },
                        ],
                      },
                    ],
                  },
                  http_method: "POST",
                  request_parameters: {},
                  request_headers: {},
                  request_body_json: {},
                },
                session_token_path: ["id"],
                expiration_duration: "P2W",
                request_authentication: {
                  type: "ApiKey",
                  inject_into: {
                    type: "RequestOption",
                    field_name: "X-Session-Token",
                    inject_into: "header",
                  },
                },
              },
            },
          },
        }),
      ],
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    expect(formValues.global.authenticator).toEqual({
      type: "SessionTokenAuthenticator",
      login_requester: {
        url: "https://api.com/session",
        authenticator: {
          type: "NoAuth",
        },
        httpMethod: "POST",
        requestOptions: {
          requestParameters: [],
          requestHeaders: [],
          requestBody: {
            type: "json_list",
            values: [],
          },
        },
        errorHandler: [
          {
            type: "DefaultErrorHandler",
            backoff_strategy: {
              type: "ConstantBackoffStrategy",
              backoff_time_in_seconds: 5,
            },
          },
        ],
      },
      decoder: "JSON",
      session_token_path: ["id"],
      expiration_duration: "P2W",
      request_authentication: {
        type: "ApiKey",
        inject_into: {
          type: "RequestOption",
          field_name: "X-Session-Token",
          inject_into: "header",
        },
      },
    });
  });

  it("unrecognized stream fields are placed into unknownFields in BuilderStream", async () => {
    const unknownFields = {
      top_level_unknown_field: {
        inner_unknown_field_1: "a",
        inner_unknown_field_2: 1,
      },
      retriever: {
        retriever_unknown_field: "b",
        requester: {
          requester_unknown_field: {
            inner_requester_field_1: "c",
            inner_requester_field_2: 2,
          },
        },
      },
    };
    const manifest: ConnectorManifest = {
      ...baseManifest,
      streams: [merge({}, stream1, unknownFields)],
      spec: apiTokenSpec,
    };
    const formValues = await convertToBuilderFormValues(noOpResolve, manifest, DEFAULT_CONNECTOR_NAME);
    expect(formValues.streams[0].unknownFields).toEqual(dump(unknownFields));
  });
});
