import { Meta, StoryFn } from "@storybook/react";

import { Icon } from "./Icon";

export default {
  title: "UI/Icon",
  component: Icon,
} as Meta<typeof Icon>;

const Template: StoryFn<typeof Icon> = (args) => (
  <>
    <Icon {...args} />
    <p>
      Next to <Icon {...args} /> some text
    </p>
  </>
);

export const Primary = Template.bind({});
Primary.args = {
  type: "arrowRight",
  color: "primary",
};

export const WithBackground = Template.bind({});
WithBackground.args = {
  type: "arrowRight",
  color: "success",
  withBackground: true,
};
