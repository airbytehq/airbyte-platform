import { ConfigTemplateList, ConfigTemplateRead } from "core/api/types/AirbyteClient";

export const mockTemplateForGDrive: ConfigTemplateRead = {
  id: "3",
  name: "GDrive",
  icon: "https://connectors.airbyte.com/files/metadata/airbyte/source-google-drive/latest/icon.svg",
  sourceDefinitionId: "312341234",
  configTemplateSpec: {
    advancedAuth: {
      authFlowType: "oauth2.0",
      predicateKey: ["credentials", "auth_type"],
      predicateValue: "Client",
      oauthConfigSpecification: {
        completeOAuthOutputSpecification: {
          type: "object",
          properties: {
            refresh_token: {
              type: "string",
              path_in_oauth_response: ["refresh_token"],
              path_in_connector_config: ["credentials", "refresh_token"],
            },
          },
          additionalProperties: false,
        },
        completeOAuthServerInputSpecification: {
          type: "object",
          properties: {
            client_id: {
              type: "string",
            },
            client_secret: {
              type: "string",
            },
          },
          additionalProperties: false,
        },
        completeOAuthServerOutputSpecification: {
          type: "object",
          properties: {
            client_id: {
              type: "string",
              path_in_connector_config: ["credentials", "client_id"],
            },
            client_secret: {
              type: "string",
              path_in_connector_config: ["credentials", "client_secret"],
            },
          },
          additionalProperties: false,
        },
      },
    },
    advancedAuthGlobalCredentialsAvailable: true,
    connectionSpecification: {
      type: "object",
      title: "Google Drive Source Spec",
      required: ["streams", "folder_url", "credentials"],
      properties: {
        streams: {
          type: "array",
          items: {
            type: "object",
            title: "FileBasedStreamConfig",
            required: ["name", "format"],
            properties: {
              name: {
                type: "string",
                title: "Name",
                description: "The name of the stream.",
              },
              globs: {
                type: "array",
                items: {
                  type: "string",
                },
                order: 1,
                title: "Globs",
                default: ["**"],
                description:
                  'The pattern used to specify which files should be selected from the file system. For more information on glob pattern matching look <a href="https://en.wikipedia.org/wiki/Glob_(programming)">here</a>.',
              },
              format: {
                type: "object",
                oneOf: [
                  {
                    type: "object",
                    title: "Avro Format",
                    required: ["filetype"],
                    properties: {
                      filetype: {
                        type: "string",
                        const: "avro",
                        title: "Filetype",
                        default: "avro",
                      },
                      double_as_string: {
                        type: "boolean",
                        title: "Convert Double Fields to Strings",
                        default: false,
                        description:
                          "Whether to convert double fields to strings. This is recommended if you have decimal numbers with a high degree of precision because there can be a loss precision when handling floating point numbers.",
                      },
                    },
                  },
                  {
                    type: "object",
                    title: "CSV Format",
                    required: ["filetype"],
                    properties: {
                      encoding: {
                        type: "string",
                        title: "Encoding",
                        default: "utf8",
                        description:
                          'The character encoding of the CSV data. Leave blank to default to <strong>UTF8</strong>. See <a href="https://docs.python.org/3/library/codecs.html#standard-encodings" target="_blank">list of python encodings</a> for allowable options.',
                      },
                      filetype: {
                        type: "string",
                        const: "csv",
                        title: "Filetype",
                        default: "csv",
                      },
                      delimiter: {
                        type: "string",
                        title: "Delimiter",
                        default: ",",
                        description:
                          "The character delimiting individual cells in the CSV data. This may only be a 1-character string. For tab-delimited data enter '\\t'.",
                      },
                      quote_char: {
                        type: "string",
                        title: "Quote Character",
                        default: '"',
                        description:
                          "The character used for quoting CSV values. To disallow quoting, make this field blank.",
                      },
                      escape_char: {
                        type: "string",
                        title: "Escape Character",
                        description:
                          "The character used for escaping special characters. To disallow escaping, leave this field blank.",
                      },
                      null_values: {
                        type: "array",
                        items: {
                          type: "string",
                        },
                        title: "Null Values",
                        default: [],
                        description:
                          "A set of case-sensitive strings that should be interpreted as null values. For example, if the value 'NA' should be interpreted as null, enter 'NA' in this field.",
                        uniqueItems: true,
                      },
                      true_values: {
                        type: "array",
                        items: {
                          type: "string",
                        },
                        title: "True Values",
                        default: ["y", "yes", "t", "true", "on", "1"],
                        description: "A set of case-sensitive strings that should be interpreted as true values.",
                        uniqueItems: true,
                      },
                      double_quote: {
                        type: "boolean",
                        title: "Double Quote",
                        default: true,
                        description: "Whether two quotes in a quoted CSV value denote a single quote in the data.",
                      },
                      false_values: {
                        type: "array",
                        items: {
                          type: "string",
                        },
                        title: "False Values",
                        default: ["n", "no", "f", "false", "off", "0"],
                        description: "A set of case-sensitive strings that should be interpreted as false values.",
                        uniqueItems: true,
                      },
                      header_definition: {
                        type: "object",
                        oneOf: [
                          {
                            type: "object",
                            title: "From CSV",
                            required: ["header_definition_type"],
                            properties: {
                              header_definition_type: {
                                type: "string",
                                const: "From CSV",
                                title: "Header Definition Type",
                                default: "From CSV",
                              },
                            },
                          },
                          {
                            type: "object",
                            title: "Autogenerated",
                            required: ["header_definition_type"],
                            properties: {
                              header_definition_type: {
                                type: "string",
                                const: "Autogenerated",
                                title: "Header Definition Type",
                                default: "Autogenerated",
                              },
                            },
                          },
                          {
                            type: "object",
                            title: "User Provided",
                            required: ["column_names", "header_definition_type"],
                            properties: {
                              column_names: {
                                type: "array",
                                items: {
                                  type: "string",
                                },
                                title: "Column Names",
                                description: "The column names that will be used while emitting the CSV records",
                              },
                              header_definition_type: {
                                type: "string",
                                const: "User Provided",
                                title: "Header Definition Type",
                                default: "User Provided",
                              },
                            },
                          },
                        ],
                        title: "CSV Header Definition",
                        default: {
                          header_definition_type: "From CSV",
                        },
                        description:
                          "How headers will be defined. `User Provided` assumes the CSV does not have a header row and uses the headers provided and `Autogenerated` assumes the CSV does not have a header row and the CDK will generate headers using for `f{i}` where `i` is the index starting from 0. Else, the default behavior is to use the header from the CSV file. If a user wants to autogenerate or provide column names for a CSV having headers, they can skip rows.",
                      },
                      strings_can_be_null: {
                        type: "boolean",
                        title: "Strings Can Be Null",
                        default: true,
                        description:
                          "Whether strings can be interpreted as null values. If true, strings that match the null_values set will be interpreted as null. If false, strings that match the null_values set will be interpreted as the string itself.",
                      },
                      skip_rows_after_header: {
                        type: "integer",
                        title: "Skip Rows After Header",
                        default: 0,
                        description: "The number of rows to skip after the header row.",
                      },
                      skip_rows_before_header: {
                        type: "integer",
                        title: "Skip Rows Before Header",
                        default: 0,
                        description:
                          "The number of rows to skip before the header row. For example, if the header row is on the 3rd row, enter 2 in this field.",
                      },
                      ignore_errors_on_fields_mismatch: {
                        type: "boolean",
                        title: "Ignore errors on field mismatch",
                        default: false,
                        description:
                          "Whether to ignore errors that occur when the number of fields in the CSV does not match the number of columns in the schema.",
                      },
                    },
                  },
                  {
                    type: "object",
                    title: "Jsonl Format",
                    required: ["filetype"],
                    properties: {
                      filetype: {
                        type: "string",
                        const: "jsonl",
                        title: "Filetype",
                        default: "jsonl",
                      },
                    },
                  },
                  {
                    type: "object",
                    title: "Parquet Format",
                    required: ["filetype"],
                    properties: {
                      filetype: {
                        type: "string",
                        const: "parquet",
                        title: "Filetype",
                        default: "parquet",
                      },
                      decimal_as_float: {
                        type: "boolean",
                        title: "Convert Decimal Fields to Floats",
                        default: false,
                        description:
                          "Whether to convert decimal fields to floats. There is a loss of precision when converting decimals to floats, so this is not recommended.",
                      },
                    },
                  },
                  {
                    type: "object",
                    title: "Unstructured Document Format",
                    required: ["filetype"],
                    properties: {
                      filetype: {
                        type: "string",
                        const: "unstructured",
                        title: "Filetype",
                        default: "unstructured",
                      },
                      strategy: {
                        enum: ["auto", "fast", "ocr_only", "hi_res"],
                        type: "string",
                        order: 0,
                        title: "Parsing Strategy",
                        default: "auto",
                        always_show: true,
                        description:
                          "The strategy used to parse documents. `fast` extracts text directly from the document which doesn't work for all files. `ocr_only` is more reliable, but slower. `hi_res` is the most reliable, but requires an API key and a hosted instance of unstructured and can't be used with local mode. See the unstructured.io documentation for more details: https://unstructured-io.github.io/unstructured/core/partition.html#partition-pdf",
                      },
                      processing: {
                        type: "object",
                        oneOf: [
                          {
                            type: "object",
                            title: "Local",
                            required: ["mode"],
                            properties: {
                              mode: {
                                enum: ["local"],
                                type: "string",
                                const: "local",
                                title: "Mode",
                                default: "local",
                              },
                            },
                            description:
                              "Process files locally, supporting `fast` and `ocr` modes. This is the default option.",
                          },
                        ],
                        title: "Processing",
                        default: {
                          mode: "local",
                        },
                        description: "Processing configuration",
                      },
                      skip_unprocessable_files: {
                        type: "boolean",
                        title: "Skip Unprocessable Files",
                        default: true,
                        always_show: true,
                        description:
                          "If true, skip files that cannot be parsed and pass the error message along as the _ab_source_file_parse_error field. If false, fail the sync.",
                      },
                    },
                    description:
                      "Extract text from document formats (.pdf, .docx, .md, .pptx) and emit as one record per file.",
                  },
                  {
                    type: "object",
                    title: "Excel Format",
                    required: ["filetype"],
                    properties: {
                      filetype: {
                        type: "string",
                        const: "excel",
                        title: "Filetype",
                        default: "excel",
                      },
                    },
                  },
                ],
                title: "Format",
                description:
                  "The configuration options that are used to alter how to read incoming files that deviate from the standard formatting.",
              },
              schemaless: {
                type: "boolean",
                title: "Schemaless",
                default: false,
                description: "When enabled, syncs will not validate or structure records against the stream's schema.",
              },
              primary_key: {
                type: "string",
                title: "Primary Key",
                description:
                  "The column or columns (for a composite key) that serves as the unique identifier of a record. If empty, the primary key will default to the parser's default primary key.",
                airbyte_hidden: true,
              },
              input_schema: {
                type: "string",
                title: "Input Schema",
                description:
                  "The schema that will be used to validate records extracted from the file. This will override the stream schema that is auto-detected from incoming files.",
              },
              validation_policy: {
                enum: ["Emit Record", "Skip Record", "Wait for Discover"],
                title: "Validation Policy",
                default: "Emit Record",
                description:
                  "The name of the validation policy that dictates sync behavior when a record does not adhere to the stream schema.",
              },
              days_to_sync_if_history_is_full: {
                type: "integer",
                title: "Days To Sync If History Is Full",
                default: 3,
                description:
                  "When the state history of the file store is full, syncs will only read files that were last modified in the provided day range.",
              },
              recent_n_files_to_read_for_schema_discovery: {
                type: "integer",
                title: "Files To Read For Schema Discover",
                description: "The number of resent files which will be used to discover the schema for this stream.",
                exclusiveMinimum: 0,
              },
            },
          },
          order: 10,
          title: "The list of streams to sync",
          description:
            'Each instance of this configuration defines a <a href="https://docs.airbyte.com/cloud/core-concepts#stream">stream</a>. Use this to define which files belong in the stream, their format, and how they should be parsed and validated. When sending data to warehouse destination such as Snowflake or BigQuery, each stream is a separate table.',
        },
        folder_url: {
          type: "string",
          order: 0,
          title: "Folder Url",
          pattern: "^https://drive.google.com/.+",
          examples: ["https://drive.google.com/drive/folders/1Xaz0vXXXX2enKnNYU5qSt9NS70gvMyYn"],
          description:
            "URL for the folder you want to sync. Using individual streams and glob patterns, it's possible to only sync a subset of all files located in the folder.",
          pattern_descriptor: "https://drive.google.com/drive/folders/MY-FOLDER-ID",
        },
        start_date: {
          type: "string",
          order: 1,
          title: "Start Date",
          format: "date-time",
          pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}Z$",
          examples: ["2021-01-01T00:00:00.000000Z"],
          description:
            "UTC date and time in the format 2017-01-25T00:00:00.000000Z. Any file modified before this date will not be replicated.",
          pattern_descriptor: "YYYY-MM-DDTHH:mm:ss.SSSSSSZ",
        },
        credentials: {
          type: "object",
          oneOf: [
            {
              type: "object",
              title: "Authenticate via Google (OAuth)",
              required: ["client_id", "client_secret", "refresh_token", "auth_type"],
              properties: {
                auth_type: {
                  enum: ["Client"],
                  type: "string",
                  const: "Client",
                  title: "Auth Type",
                  default: "Client",
                },
                client_id: {
                  type: "string",
                  title: "Client ID",
                  description: "Client ID for the Google Drive API",
                  airbyte_secret: true,
                },
                client_secret: {
                  type: "string",
                  title: "Client Secret",
                  description: "Client Secret for the Google Drive API",
                  airbyte_secret: true,
                },
                refresh_token: {
                  type: "string",
                  title: "Refresh Token",
                  description: "Refresh Token for the Google Drive API",
                  airbyte_secret: true,
                },
              },
            },
            {
              type: "object",
              title: "Service Account Key Authentication",
              required: ["service_account_info", "auth_type"],
              properties: {
                auth_type: {
                  enum: ["Service"],
                  type: "string",
                  const: "Service",
                  title: "Auth Type",
                  default: "Service",
                },
                service_account_info: {
                  type: "string",
                  title: "Service Account Information",
                  description:
                    'The JSON key of the service account to use for authorization. Read more <a href="https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating_service_account_keys">here</a>.',
                  airbyte_secret: true,
                },
              },
            },
          ],
          title: "Authentication",
          description: "Credentials for connecting to the Google Drive API",
        },
        delivery_method: {
          type: "object",
          group: "advanced",
          oneOf: [
            {
              type: "object",
              title: "Replicate Records",
              required: ["delivery_type"],
              properties: {
                delivery_type: {
                  enum: ["use_records_transfer"],
                  type: "string",
                  const: "use_records_transfer",
                  title: "Delivery Type",
                  default: "use_records_transfer",
                },
              },
              description:
                "Recommended - Extract and load structured records into your destination of choice. This is the classic method of moving data in Airbyte. It allows for blocking and hashing individual fields or files from a structured schema. Data can be flattened, typed and deduped depending on the destination.",
            },
            {
              type: "object",
              title: "Copy Raw Files",
              required: ["delivery_type"],
              properties: {
                delivery_type: {
                  enum: ["use_file_transfer"],
                  type: "string",
                  const: "use_file_transfer",
                  title: "Delivery Type",
                  default: "use_file_transfer",
                },
                preserve_directory_structure: {
                  type: "boolean",
                  title: "Preserve Sub-Directories in File Paths",
                  default: true,
                  description:
                    "If enabled, sends subdirectory folder structure along with source file names to the destination. Otherwise, files will be synced by their names only. This option is ignored when file-based replication is not enabled.",
                },
              },
              description:
                "Copy raw files without parsing their contents. Bits are copied into the destination exactly as they appeared in the source. Recommended for use with unstructured text data, non-text and compressed files.",
            },
            {
              type: "object",
              title: "Replicate Permissions ACL",
              required: ["delivery_type"],
              properties: {
                domain: {
                  type: "string",
                  order: 1,
                  title: "Domain",
                  description: "The Google domain of the identities.",
                  airbyte_hidden: false,
                },
                delivery_type: {
                  enum: ["use_permissions_transfer"],
                  type: "string",
                  const: "use_permissions_transfer",
                  title: "Delivery Type",
                  default: "use_permissions_transfer",
                },
                include_identities_stream: {
                  type: "boolean",
                  title: "Include Identity Stream",
                  default: true,
                  description:
                    "This data can be used in downstream systems to recreate permission restrictions mirroring the original source",
                },
              },
              description:
                "Sends one identity stream and one for more permissions (ACL) streams to the destination. This data can be used in downstream systems to recreate permission restrictions mirroring the original source.",
            },
          ],
          order: 1,
          title: "Delivery Method",
          default: "use_records_transfer",
          display_type: "radio",
        },
      },
      description:
        "Used during spec; allows the developer to configure the cloud provider specific options\nthat are needed when users configure a file-based source.",
    },
  },
};

export const mockConfigTemplateList: ConfigTemplateList = {
  configTemplates: [
    {
      id: "1",
      name: "FakerOne",
      icon: "https://connectors.airbyte.com/files/metadata/airbyte/source-faker/latest/icon.svg",
    },
    {
      id: "2",
      name: "Also Faker!",
      icon: "https://connectors.airbyte.com/files/metadata/airbyte/source-faker/latest/icon.svg",
    },
    {
      id: "3",
      name: "GDrive",
      icon: "https://connectors.airbyte.com/files/metadata/airbyte/source-google-drive/latest/icon.svg",
    },
  ],
};

export const mockConfigTemplateFakerOne: ConfigTemplateRead = {
  id: "1",
  name: "FakerOne",
  icon: "https://connectors.airbyte.com/files/metadata/airbyte/source-faker/latest/icon.svg",
  sourceDefinitionId: "actor-definition-id",
  configTemplateSpec: {
    connectionSpecification: {
      type: "object",
      title: "Faker Source Spec",
      $schema: "http://json-schema.org/draft-07/schema#",
      required: ["seed"],
      properties: {
        seed: {
          type: "integer",
          order: 1,
          title: "Seed",
          default: -1,
          description: "A very special description just for Airbyte Embedded!",
        },
      },
    },
  },
};

export const mockConfigTemplateAlsoFaker: ConfigTemplateRead = {
  id: "2",
  name: "Also Faker!",
  icon: "https://connectors.airbyte.com/files/metadata/airbyte/source-faker/latest/icon.svg",
  sourceDefinitionId: "actor-definition-id",
  configTemplateSpec: {
    connectionSpecification: {
      type: "object",
      title: "Faker Source Spec",
      $schema: "http://json-schema.org/draft-07/schema#",
      required: ["seed"],
      properties: {
        seed: {
          type: "integer",
          order: 1,
          title: "Seed",
          default: -1,
          description: "A very special description just for Airbyte Embedded!",
        },
      },
    },
  },
};
