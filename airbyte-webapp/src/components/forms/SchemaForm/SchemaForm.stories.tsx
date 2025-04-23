import { StoryObj } from "@storybook/react";
import { FromSchema } from "json-schema-to-ts";
import { useWatch } from "react-hook-form";

import { formatJson } from "components/connectorBuilder/utils";
import { Card } from "components/ui/Card";

import { ConfirmationModalService } from "hooks/services/ConfirmationModal";
import { NotificationService } from "hooks/services/Notification";

import { SchemaFormControl } from "./Controls/SchemaFormControl";
import { SchemaForm } from "./SchemaForm";
import { SchemaFormRemainingFields } from "./SchemaFormRemainingFields";
import declarativeComponentSchema from "../../../../build/declarative_component_schema.yaml";
import { FormControl } from "../FormControl";
import { FormSubmissionButtons } from "../FormSubmissionButtons";

export default {
  title: "SchemaForm",
  component: SchemaForm,
} as StoryObj<typeof SchemaForm>;

const schema = {
  type: "object",
  definitions: {
    address: {
      type: "object",
      title: "Address",
      description: "Your home address",
      required: ["street", "city"],
      properties: {
        street: { type: "string", title: "Street", default: "123 Main St" },
        city: { type: "string", title: "City" },
        deliveryInstructions: {
          type: "object",
          title: "Delivery Instructions",
          description: "Enter your delivery instructions",
          required: ["dropOff"],
          properties: {
            dropOff: { type: "string", title: "Drop Off" },
            pickUp: { type: "string", title: "Pickup" },
          },
        },
      },
    },
  },
  properties: {
    name: {
      type: "string",
      title: "Name",
      default: "John Doe",
      multiline: true,
      description: "Enter your first name",
    },
    favoriteColors: {
      type: "array",
      title: "Favorite Colors",
      items: {
        type: "string",
      },
    },
    age: { type: "integer", title: "Age", minimum: 0, default: 23 },
    ageGroup: {
      type: "string",
      title: "Age Group",
      enum: ["child", "adult", "senior"],
    },
    address: {
      $ref: "#/definitions/address",
    },
    contactMethod: {
      type: "object",
      title: "Contact Method",
      description: "How you should be contacted",
      oneOf: [
        {
          title: "Email",
          required: ["emailAddress", "frequency"],
          properties: {
            type: {
              type: "string",
              enum: ["EmailContactMethod"],
            },
            emailAddress: { type: "string", title: "Email Address", format: "email", default: "test@test.com" },
            frequency: { type: "string", title: "Email Frequency", enum: ["daily", "weekly", "monthly"] },
            specialRequests: {
              type: "array",
              title: "Special Requests",
              description: "Special requests for contacting",
              maxItems: 2,
              items: {
                type: "object",
                default: {
                  type: "HolidaySpecialRequest",
                  holiday: "Christmas",
                },
                anyOf: [
                  {
                    title: "Holidays",
                    properties: {
                      type: {
                        type: "string",
                        enum: ["HolidaySpecialRequest"],
                      },
                      holiday: { type: "string", title: "Holiday" },
                    },
                    required: ["holiday"],
                  },
                  {
                    title: "Birthdays",
                    properties: {
                      type: {
                        type: "string",
                        enum: ["BirthdaySpecialRequest"],
                      },
                      birthday: { type: "string", title: "Birthday" },
                    },
                    required: ["birthday"],
                  },
                ],
              },
            },
          },
        },
        {
          title: "SMS",
          properties: {
            type: {
              type: "string",
              enum: ["SMSContactMethod"],
            },
            phoneNumber: { type: "string", title: "Phone Number" },
            frequency: { type: "string", title: "SMS Frequency", enum: ["hourly", "daily", "weekly", "monthly"] },
          },
          required: ["phoneNumber"],
        },
      ],
    },
    friends: {
      type: "array",
      title: "Friends",
      maxItems: 3,
      items: {
        type: "object",
        title: "Friend",
        required: ["name", "age"],
        properties: {
          name: { type: "string", title: "Name", minLength: 2 },
          age: { type: "integer", title: "Age" },
          address: {
            $ref: "#/definitions/address",
          },
        },
        additionalProperties: false,
      },
    },
  },
  required: ["favoriteColors"],
  additionalProperties: false,
} as const;

const onSubmit = async (values: FromSchema<typeof schema>) => {
  console.log(values);
};

/**
 * Default story showing standard behavior with auto-generated form fields
 */
export const Default = () => (
  <Card>
    <SchemaForm schema={schema} onSubmit={onSubmit}>
      <SchemaFormControl />
      <FormSubmissionButtons allowInvalidSubmit allowNonDirtySubmit />
      <ShowFormValues />
    </SchemaForm>
  </Card>
);

/**
 * Shows how SchemaFormRemainingFields can be used to render fields that haven't been explicitly rendered
 */
export const RemainingFields = () => (
  <Card>
    <SchemaForm schema={schema} onSubmit={onSubmit}>
      <div style={{ marginBottom: "20px" }}>
        <h3>Explicitly Rendered Fields:</h3>
        <SchemaFormControl path="name" />
        <SchemaFormControl path="age" />
        <SchemaFormControl path="friends" />
      </div>

      <div style={{ borderTop: "1px solid #ddd", paddingTop: "20px" }}>
        <h3>Remaining Unrendered Fields:</h3>
        {/* 
          Fields are registered synchronously during render,
          so there's no need for a delay to prevent flashing content
        */}
        <SchemaFormRemainingFields />
      </div>

      <FormSubmissionButtons />
      <ShowFormValues />
    </SchemaForm>
  </Card>
);

/**
 * Shows how to override specific fields with custom components
 */
export const OverrideByPath = () => (
  <Card>
    <SchemaForm schema={schema} onSubmit={onSubmit}>
      <SchemaFormControl
        overrideByPath={{
          "address.deliveryInstructions.dropOff": (
            <FormControl
              name="address.deliveryInstructions.dropOff"
              label="Drop Off (custom)"
              fieldType="input"
              optional
            />
          ),
          // Example of hiding a field by setting it to null
          ageGroup: null,
        }}
      />
      <FormSubmissionButtons />
      <ShowFormValues />
    </SchemaForm>
  </Card>
);

/**
 * Shows how fields can be linked via reference handling
 */
export const RefHandling = () => {
  const refSchema = {
    type: "object",
    properties: {
      definitions: {
        type: "object",
        properties: {
          shared: {
            type: "object",
          },
        },
      },
      streams: {
        type: "array",
        title: "Streams",
        items: {
          type: "object",
          properties: {
            name: { type: "string", title: "Name", description: "The name of the stream" },
            url: { type: "string", linkable: true, title: "URL", description: "The URL of the stream" },
            authentication: {
              type: "object",
              description: "The authentication details for the stream",
              linkable: true,
              properties: {
                username: { type: "string", title: "Username" },
                password: { type: "string", title: "Password" },
              },
              required: ["username", "password"],
            },
            request_parameters: {
              title: "Query Parameters",
              description:
                "Specifies the query parameters that should be set on an outgoing HTTP request given the inputs.",
              anyOf: [
                {
                  type: "string",
                  title: "Interpolated Value",
                },
                {
                  type: "number",
                  title: "Interpolated Number",
                },
                {
                  type: "object",
                  title: "Key/Value Pairs",
                  // properties: {
                  //   id: { type: "string", title: "ID" },
                  //   name: { type: "string", title: "Name" },
                  // },
                  // additionalProperties: {
                  //   anyOf: [{ type: "string" }, { $ref: "#/definitions/QueryProperties" }],
                  // },
                },
              ],
            },
          },
          required: ["name", "url"],
          additionalProperties: false,
        },
      },
    },
    definitions: {
      QueryProperties: {
        title: "Query Properties",
        description:
          "For APIs that require explicit specification of the properties to query for, this component specifies which property fields and how they are supplied to outbound requests.",
        type: "object",
        required: ["type", "property_list"],
        properties: {
          type: {
            type: "string",
            enum: ["QueryProperties"],
          },
          property_list: {
            title: "Property List",
            description:
              "The set of properties that will be queried for in the outbound request. This can either be statically defined or dynamic based on an API endpoint",
            anyOf: [
              {
                type: "array",
                items: {
                  type: "string",
                },
              },
              {
                $ref: "#/definitions/PropertiesFromEndpoint",
              },
            ],
          },
          always_include_properties: {
            title: "Always Include Properties",
            description:
              "The list of properties that should be included in every set of properties when multiple chunks of properties are being requested.",
            type: "array",
            items: {
              type: "string",
            },
          },
          property_chunking: {
            title: "Property Chunking",
            description:
              "Defines how query properties will be grouped into smaller sets for APIs with limitations on the number of properties fetched per API request.",
            $ref: "#/definitions/PropertyChunking",
          },
        },
      },
      PropertiesFromEndpoint: {
        title: "Properties from Endpoint",
        description:
          "Defines the behavior for fetching the list of properties from an API that will be loaded into the requests to extract records.",
        type: "object",
        required: ["type", "property_field_path", "retriever"],
        properties: {
          type: {
            type: "string",
            enum: ["PropertiesFromEndpoint"],
          },
          property_field_path: {
            description: "Describes the path to the field that should be extracted",
            type: "array",
            items: {
              type: "string",
            },
            examples: [["name"]],
            interpolation_context: ["config", "parameters"],
          },
          // retriever: {
          //   description:
          //     "Requester component that describes how to fetch the properties to query from a remote API endpoint.",
          //   anyOf: [
          //     {
          //       $ref: "#/definitions/CustomRetriever",
          //     },
          //     {
          //       $ref: "#/definitions/SimpleRetriever",
          //     },
          //   ],
          // },
        },
      },
    },
    additionalProperties: false,
    required: ["streams"],
  } as const;

  return (
    <NotificationService>
      <ConfirmationModalService>
        <SchemaForm
          schema={refSchema}
          onSubmit={async (values: FromSchema<typeof refSchema>) => console.log(values)}
          refTargetPath="shared"
          initialValues={{
            streams: [
              {
                name: "Users",
                url: "https://users.com",
                authentication: {
                  username: "users_username",
                  password: "users_password",
                },
              },
              {
                name: "Products",
                url: "https://products.com",
                authentication: {
                  username: "products_username",
                  password: "products_password",
                },
              },
            ],
          }}
        >
          <Card>
            <SchemaFormControl path="streams" />
            <ShowFormValues />
            <FormSubmissionButtons allowInvalidSubmit allowNonDirtySubmit />
          </Card>
        </SchemaForm>
      </ConfirmationModalService>
    </NotificationService>
  );
};

export const DeclarativeComponentSchema = () => {
  return (
    <SchemaForm
      schema={declarativeComponentSchema}
      onSubmit={async (values) => {
        console.log("submitted values", values);
      }}
      initialValues={{
        type: "DeclarativeSource",
        check: {
          streams: [],
        },
        version: "1.0.0",
        streams: [
          {
            name: "pokemon",
            type: "DeclarativeStream",
            retriever: {
              type: "SimpleRetriever",
              decoder: {
                type: "JsonDecoder",
              },
              paginator: {
                type: "DefaultPaginator",
                page_size_option: {
                  type: "RequestOption",
                  field_name: "limit",
                  inject_into: "request_parameter",
                },
                page_token_option: {
                  type: "RequestOption",
                  field_name: "offset",
                  inject_into: "request_parameter",
                },
                pagination_strategy: {
                  type: "OffsetIncrement",
                  page_size: 10,
                },
              },
              requester: {
                type: "HttpRequester",
                url_base: "https://pokeapi.co/api/v2/",
                path: "pokemon",
                http_method: "GET",
              },
              record_selector: {
                type: "RecordSelector",
                extractor: {
                  type: "DpathExtractor",
                  field_path: ["results"],
                },
              },
            },
          },
        ],
      }}
    >
      <Card>
        <SchemaFormControl path="streams.0" />
        <FormSubmissionButtons allowInvalidSubmit allowNonDirtySubmit />
        <ShowFormValues />
      </Card>
    </SchemaForm>
  );
};

export const Test = () => {
  return (
    <SchemaForm
      schema={{
        type: "object",
        properties: {
          test: {
            anyOf: [
              { type: "string" },
              {
                title: "Request Parameters",
                anyOf: [
                  {
                    type: "number",
                  },
                  {
                    type: "object",
                    additionalProperties: {
                      anyOf: [
                        { type: "string" },
                        {
                          type: "object",
                          properties: {
                            innerString: { type: "string" },
                            innerNumber: { type: "number" },
                          },
                        },
                      ],
                    },
                  },
                ],
              },
            ],
          },
        },
      }}
      onSubmit={onSubmit}
    >
      <Card>
        <SchemaFormControl />
        <FormSubmissionButtons allowInvalidSubmit allowNonDirtySubmit />
        <ShowFormValues />
      </Card>
    </SchemaForm>
  );
};

export const Test2 = () => {
  return (
    <SchemaForm
      schema={{
        type: "object",
        properties: {
          primary_key: {
            anyOf: [
              { type: "string", title: "Single Key" },
              {
                type: "array",
                title: "Composite Key",
                items: {
                  type: "string",
                },
              },
              {
                type: "array",
                title: "Composite Nested Keys",
                items: {
                  type: "array",
                  items: {
                    type: "string",
                  },
                },
              },
            ],
          },
        },
        required: ["primary_key"],
      }}
      onSubmit={onSubmit}
    >
      <Card>
        <SchemaFormControl />
        <FormSubmissionButtons allowInvalidSubmit allowNonDirtySubmit />
        <ShowFormValues />
      </Card>
    </SchemaForm>
  );
};

const ShowFormValues = () => {
  const values = useWatch();
  return <pre>{formatJson(values)}</pre>;
};
