import { Meta, StoryFn } from "@storybook/react";

import { Icon } from "./Icon";
import { Heading } from "../Heading";

export default {
  title: "UI/Icon",
  component: Icon,
} as Meta<typeof Icon>;

const Template: StoryFn<typeof Icon> = (args) => (
  <div>
    <Heading as="h2" size="sm">
      xs
    </Heading>
    <Icon {...args} size="xs" />
    <p style={{ background: "#eee" }}>
      Next to <Icon {...args} size="xs" /> some text
    </p>
    <Heading as="h2" size="sm">
      sm
    </Heading>
    <Icon {...args} size="sm" />
    <p style={{ background: "#eee" }}>
      Next to <Icon {...args} size="sm" /> some text
    </p>
    <Heading as="h2" size="sm">
      md
    </Heading>
    <Icon {...args} />
    <p style={{ background: "#eee" }}>
      Next to <Icon {...args} size="md" /> some text
    </p>
    <Heading as="h2" size="sm">
      lg
    </Heading>
    <Icon {...args} size="lg" />
    <p style={{ background: "#eee" }}>
      Next to <Icon {...args} size="lg" /> some text
    </p>
    <Heading as="h2" size="sm">
      xl
    </Heading>
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
Primary.parameters = {
  controls: { exclude: ["size"] },
};

export const WithBackground = Template.bind({});
WithBackground.args = {
  type: "arrowRight",
  color: "success",
  withBackground: true,
};
WithBackground.parameters = {
  controls: { exclude: ["size"] },
};
