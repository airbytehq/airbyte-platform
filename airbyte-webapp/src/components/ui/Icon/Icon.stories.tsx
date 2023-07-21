import { Meta, StoryFn } from "@storybook/react";

import { Icon } from "./Icon";

export default {
  title: "UI/Icon",
  component: Icon,
} as Meta<typeof Icon>;

const Template: StoryFn<typeof Icon> = (args) => (
  <div>
    <Icon {...args} size="xs" />
    <p style={{ background: "#eee" }}>
      Next to <Icon {...args} size="xs" /> some text
    </p>
    <Icon {...args} size="sm" />
    <p style={{ background: "#eee" }}>
      Next to <Icon {...args} size="sm" /> some text
    </p>
    <Icon {...args} />
    <p style={{ background: "#eee" }}>
      Next to <Icon {...args} /> some text
    </p>
    <Icon {...args} size="lg" />
    <p style={{ background: "#eee" }}>
      Next to <Icon {...args} size="lg" /> some text
    </p>
    <Icon {...args} size="xl" />
    <p style={{ background: "#eee" }}>
      Next to <Icon {...args} size="xl" /> some text
    </p>
  </div>
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
