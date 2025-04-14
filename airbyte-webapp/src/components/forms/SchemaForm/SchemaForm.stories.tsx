import { StoryObj } from "@storybook/react";
import { FromSchema } from "json-schema-to-ts";
import { useWatch } from "react-hook-form";

import { formatJson } from "components/connectorBuilder/utils";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

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
      type: "object",
      title: "Address",
      description: "Your home address",
      minProperties: 10,
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

const ShowFormValues = () => {
  const values = useWatch();
  return <pre>{formatJson(values)}</pre>;
};
