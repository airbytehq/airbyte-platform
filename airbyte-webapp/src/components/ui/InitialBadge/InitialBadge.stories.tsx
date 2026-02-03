import { StoryObj } from "@storybook/react";

import { InitialBadge } from "./InitialBadge";

export default {
  title: "ui/InitialBadge",
  component: InitialBadge,
  argTypes: {
    inputString: {
      control: {
        type: "text",
      },
    },
  },
} as StoryObj<typeof InitialBadge>;

export const Default: StoryObj<typeof InitialBadge> = {
  args: {
    inputString: "Octavia Squidington",
  },
  render: (args) => <InitialBadge {...args} />,
};
