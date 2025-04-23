import { StoryObj } from "@storybook/react";
import { FromSchema } from "json-schema-to-ts";
import { useWatch } from "react-hook-form";

import { formatJson } from "components/connectorBuilder/utils";
import { Card } from "components/ui/Card";

import { ConfirmationModalService } from "hooks/services/ConfirmationModal";
import { NotificationService } from "hooks/services/Notification";

import { SchemaForm } from "./SchemaForm";
import { SchemaFormControl } from "./SchemaFormControl";
import { SchemaFormRemainingFields } from "./SchemaFormRemainingFields";
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
          </Card>
        </SchemaForm>
      </ConfirmationModalService>
    </NotificationService>
  );
};

const ShowFormValues = () => {
  const values = useWatch();
  return <pre>{formatJson(values)}</pre>;
};
