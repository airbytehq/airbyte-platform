import { Meta, StoryFn } from "@storybook/react";

import { ThemeToggle } from "./ThemeToggle";

export default {
  title: "Common/ThemeToggle",
  component: ThemeToggle,
  argTypes: {},
} as Meta<typeof ThemeToggle>;

const Template: StoryFn<typeof ThemeToggle> = (args) => <ThemeToggle {...args} />;

export const Default = Template.bind({});
Default.args = {};
