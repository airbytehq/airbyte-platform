import { StoryFn, Meta } from "@storybook/react";

import { Switch } from "./Switch";

export default {
  title: "Ui/Switch",
  component: Switch,
  argTypes: {},
} as Meta<typeof Switch>;

const Template: StoryFn<typeof Switch> = (args) => <Switch {...args} />;

export const SwitchControl = Template.bind({});
SwitchControl.args = {
  checked: false,
  size: "sm",
  loading: false,
};
