import { StoryObj } from "@storybook/react";
import { FromSchema } from "json-schema-to-ts";
import { useWatch } from "react-hook-form";

import { formatJson } from "components/connectorBuilder/utils";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { ConfirmationModalService } from "hooks/services/ConfirmationModal";
import { NotificationService } from "hooks/services/Notification";

import { SchemaForm } from "./SchemaForm";
import { SchemaFormControl } from "./SchemaFormControl";
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
        street: { type: "string", title: "Street" },
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
      minLength: 2,
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
    age: { type: "integer", title: "Age", minimum: 0, default: "23" },
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
            emailAddress: { type: "string", title: "Email Address", format: "email" },
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
        },
      ],
    },
    friends: {
      type: "array",
      title: "Friends",
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

export const Default = () => (
  <Card>
    <SchemaForm schema={schema} onSubmit={onSubmit}>
      <SchemaFormControl />
      <FormSubmissionButtons allowInvalidSubmit allowNonDirtySubmit />
      <ShowFormValues />
    </SchemaForm>
  </Card>
);

export const WithSchemaFormControl = () => (
  <Card>
    <SchemaForm schema={schema} onSubmit={onSubmit}>
      <SchemaFormControl path="name" />
      <SchemaFormControl path="age" />
      <FormSubmissionButtons />
    </SchemaForm>
  </Card>
);

export const WithOverride = () => (
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
        }}
      />
      <FormSubmissionButtons />
    </SchemaForm>
  </Card>
);

export const SeparateCards = () => (
  <SchemaForm schema={schema} onSubmit={onSubmit}>
    <FlexContainer direction="column" gap="xl">
      <Card>
        <SchemaFormControl path="name" />
        <SchemaFormControl path="age" />
        <SchemaFormControl path="friends" />
      </Card>
      <Card>
        <SchemaFormControl
          path="address"
          overrideByPath={{
            "address.deliveryInstructions": null,
          }}
        />
      </Card>
      <Card>
        <SchemaFormControl
          path="address.deliveryInstructions"
          overrideByPath={{
            "address.deliveryInstructions.pickUp": (
              <FormControl
                name="address.deliveryInstructions.pickUp"
                label="Pick Up (Custom)"
                fieldType="input"
                optional
              />
            ),
          }}
        />
      </Card>
      <Card>
        <FormSubmissionButtons />
      </Card>
      <ShowFormValues />
    </FlexContainer>
  </SchemaForm>
);

export const Streams = () => {
  const streamsSchema = {
    type: "object",
    properties: {
      definitions: {
        type: "object",
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
              // title: "Authentication",
              description: "The authentication details for the stream",
              linkable: true,
              properties: {
                username: { type: "string", title: "Username" },
                password: { type: "string", title: "Password" },
              },
              required: ["username", "password"],
            },
          },
          required: ["name", "url"],
          additionalProperties: false,
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
          schema={streamsSchema}
          onSubmit={async (values: FromSchema<typeof streamsSchema>) => console.log(values)}
          refTargetPath="definitions.shared"
          initialValues={{
            // shared: {
            //   authentication: {
            //     username: "user",
            //     password: "pass",
            //   },
            //   url: "https://shared.com",
            // },
            streams: [
              {
                name: "Users",
                url: "https://users.com",
                authentication: {
                  username: "users username",
                  password: "users password",
                },
              },
              {
                name: "Products",
                url: "https://products.com",
                authentication: {
                  username: "products username",
                  password: "products password",
                },
              },
              {
                name: "Orders",
                url: "https://orders.com",
                authentication: {
                  username: "orders username",
                  password: "orders password",
                },
              },
            ],
          }}
        >
          <Card>
            <ShowFormValues />
            <SchemaFormControl />
          </Card>
        </SchemaForm>
      </ConfirmationModalService>
    </NotificationService>
  );
};

export const MultipleRequesters = () => {
  const streamsSchema = {
    type: "object",
    properties: {
      shared: {
        type: "object",
      },
      streams: {
        type: "array",
        title: "Streams",
        items: {
          type: "object",
          properties: {
            name: { type: "string", title: "Name", description: "The name of the stream" },
            url: { type: "string", linkable: true, title: "URL", description: "The URL of the stream" },
            creationRequester: {
              type: "object",
              title: "Creation Requester",
              description: "The creation requester of the stream",
              properties: {
                authentication: {
                  type: "object",
                  title: "Authentication",
                  description: "The authentication details for the stream",
                  linkable: true,
                  properties: {
                    username: { type: "string", title: "Username" },
                    password: { type: "string", title: "Password" },
                  },
                },
              },
              required: ["authentication"],
            },
            pollingRequester: {
              type: "object",
              title: "Polling Requester",
              description: "The polling requester of the stream",
              properties: {
                authentication: {
                  type: "object",
                  title: "Authentication",
                  description: "The authentication details for the stream",
                  linkable: true,
                  properties: {
                    username: { type: "string", title: "Username" },
                    password: { type: "string", title: "Password" },
                  },
                },
              },
              required: ["authentication"],
            },
            downloadRequester: {
              type: "object",
              title: "Download Requester",
              description: "The download requester of the stream",
              properties: {
                authentication: {
                  type: "object",
                  title: "Authentication",
                  description: "The authentication details for the stream",
                  linkable: true,
                  properties: {
                    username: { type: "string", title: "Username" },
                    password: { type: "string", title: "Password" },
                  },
                },
              },
              required: ["authentication"],
            },
          },
          required: ["name", "url", "creationRequester", "pollingRequester", "downloadRequester"],
          additionalProperties: false,
        },
      },
    },
    additionalProperties: false,
    required: ["streams"],
  } as const;

  return (
    <SchemaForm
      schema={streamsSchema}
      onSubmit={async (values: FromSchema<typeof streamsSchema>) => console.log(values)}
      refTargetPath="shared"
      initialValues={{
        streams: [
          {
            name: "Users",
            url: "https://users.com",
            creationRequester: {
              authentication: {
                username: "users creation username",
                password: "users creation password",
              },
            },
            pollingRequester: {
              authentication: {
                username: "users polling username",
                password: "users polling password",
              },
            },
            downloadRequester: {
              authentication: {
                username: "users download username",
                password: "users download password",
              },
            },
          },
          {
            name: "Products",
            url: "https://products.com",
            creationRequester: {
              authentication: {
                username: "products creation username",
                password: "products creation password",
              },
            },
            pollingRequester: {
              authentication: {
                username: "products polling username",
                password: "products polling password",
              },
            },
            downloadRequester: {
              authentication: {
                username: "products download username",
                password: "products download password",
              },
            },
          },
          {
            name: "Orders",
            url: "https://orders.com",
            creationRequester: {
              authentication: {
                username: "orders creation username",
                password: "orders creation password",
              },
            },
            pollingRequester: {
              authentication: {
                username: "orders polling username",
                password: "orders polling password",
              },
            },
            downloadRequester: {
              authentication: {
                username: "orders download username",
                password: "orders download password",
              },
            },
          },
        ],
      }}
    >
      <Card>
        <ShowFormValues />
        <SchemaFormControl />
      </Card>
    </SchemaForm>
  );
};

const ShowFormValues = () => {
  const values = useWatch();
  // const { errors } = useFormState();
  // console.log(errors);
  return <pre>{formatJson(values)}</pre>;
};
