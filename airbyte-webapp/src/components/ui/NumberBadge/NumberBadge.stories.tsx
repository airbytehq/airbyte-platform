import { StoryFn, Meta } from "@storybook/react";

import { NumberBadge } from "./NumberBadge";

export default {
  title: "UI/NumberBadge",
  component: NumberBadge,
  argTypes: {},
} as Meta<typeof NumberBadge>;

const Template: StoryFn<typeof NumberBadge> = (args) => <NumberBadge {...args} />;

export const Primary = Template.bind({});
Primary.args = {
  value: 10,
};
