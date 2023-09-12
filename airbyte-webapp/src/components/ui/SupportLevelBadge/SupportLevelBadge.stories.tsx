import { Meta, StoryFn } from "@storybook/react";

import { SupportLevelBadge } from "./SupportLevelBadge";

export default {
  title: "UI/SupportLevelBadge",
  component: SupportLevelBadge,
  argTypes: {
    size: {
      options: ["small", "medium"],
      control: {
        type: "radio",
      },
    },
  },
} as Meta<typeof SupportLevelBadge>;

const Template: StoryFn<typeof SupportLevelBadge> = (args) => <SupportLevelBadge {...args} />;

export const Primary = Template.bind({});
Primary.args = {
  supportLevel: "certified",
  size: "medium",
  tooltip: true,
};
