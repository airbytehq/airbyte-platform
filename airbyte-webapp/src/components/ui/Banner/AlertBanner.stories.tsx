import { StoryFn, Meta } from "@storybook/react";

import { AlertBanner } from "./AlertBanner";

export default {
  title: "UI/AlertBanner",
  component: AlertBanner,
  argTypes: {},
} as Meta<typeof AlertBanner>;

const Template: StoryFn<typeof AlertBanner> = (args) => <AlertBanner {...args} />;

export const Primary = Template.bind({});
Primary.args = {
  message: "This is the AlertBanner component!",
};
