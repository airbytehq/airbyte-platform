import { Meta } from "@storybook/react";

import { TagBadge, TagProps } from "./TagBadge";

const meta: Meta<typeof TagBadge> = {
  title: "UI/TagBadge",
  component: TagBadge,
  argTypes: {
    color: {
      control: {
        type: "text",
      },
    },
    text: {
      control: {
        type: "text",
      },
    },
  },
};
export default meta;

export const Default = {
  args: { text: "Business", color: "FBECB1" },
  render: (args: TagProps) => <TagBadge {...args} />,
};
