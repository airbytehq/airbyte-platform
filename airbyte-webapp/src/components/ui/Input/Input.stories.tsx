import type { Meta, StoryObj } from "@storybook/react";

import { ConnectorIcon } from "components/ConnectorIcon";

import { Input } from "./Input";

const meta: Meta<typeof Input> = {
  title: "Ui/Input",
  component: Input,
  parameters: {
    layout: "centered",
  },
  tags: ["autodocs"],
  argTypes: {
    type: {
      control: "select",
      options: ["text", "number", "password", "email", "url", "search"],
      description: "The input type",
    },
    disabled: {
      control: "boolean",
      description: "Whether the input is disabled",
    },
    error: {
      control: "boolean",
      description: "Whether the input has an error state",
    },
    light: {
      control: "boolean",
      description: "Whether to use light styling",
    },
    inline: {
      control: "boolean",
      description: "Whether to display inline",
    },
    placeholder: {
      control: "text",
      description: "Placeholder text",
    },
    defaultValue: {
      control: "text",
      description: "Default value",
    },
  },
  args: {
    type: "text",
    disabled: false,
    error: false,
    light: false,
    inline: false,
    placeholder: "Enter text here...",
  },
};
export default meta;

type Story = StoryObj<typeof Input>;

export const Default: Story = {};

export const Password: Story = {
  args: {
    type: "password",
    placeholder: "Enter password...",
    defaultValue: "secretpassword123",
  },
};

export const WithError: Story = {
  args: {
    error: true,
    defaultValue: "invalid input",
  },
};

export const Disabled: Story = {
  args: {
    disabled: true,
    defaultValue: "Cannot edit this",
  },
};

export const Light: Story = {
  args: {
    light: true,
    placeholder: "Light variant...",
  },
};

export const WithIcon: Story = {
  args: {
    placeholder: "Search connectors...",
    icon: (
      <ConnectorIcon icon="https://connectors.airbyte.com/files/metadata/airbyte/source-bigquery/latest/icon.svg" />
    ),
  },
};

export const Number: Story = {
  args: {
    type: "number",
    placeholder: "Enter a number...",
    defaultValue: 42,
  },
};

export const Email: Story = {
  args: {
    type: "email",
    placeholder: "Enter your email...",
    defaultValue: "user@example.com",
  },
};
