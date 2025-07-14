import type { Meta, StoryObj } from "@storybook/react";

import { ConnectorIcon } from "components/ConnectorIcon";

import { Input } from "./Input";

const meta: Meta<typeof Input> = {
  title: "Ui/Input",
  component: Input,
  argTypes: {
    disabled: { control: "boolean" },
    type: { control: { type: "select", options: ["text", "number", "password"] } },
  },
};
export default meta;

type Story = StoryObj<typeof Input>;

export const Primary: Story = {
  args: {
    placeholder: "Enter text here...",
  },
};

export const WithIcon: Story = {
  args: {
    placeholder: "Enter text here...",
    icon: (
      <ConnectorIcon icon="https://connectors.airbyte.com/files/metadata/airbyte/source-bigquery/latest/icon.svg" />
    ),
  },
};
