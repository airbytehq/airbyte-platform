import { StoryFn, Meta } from "@storybook/react";

import { StatusIcon } from "./StatusIcon";

export default {
  title: "UI/StatusIcon",
  component: StatusIcon,
  argTypes: {
    value: { type: { name: "number", required: false } },
  },
} as Meta<typeof StatusIcon>;

const Template: StoryFn<typeof StatusIcon> = (args) => <StatusIcon {...args} />;

export const Primary = Template.bind({});
Primary.args = {
  status: "success",
};
