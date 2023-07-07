import { Meta, StoryFn } from "@storybook/react";

import { Collapsible } from "./Collapsible";

export default {
  title: "Ui/Collapsible",
  component: Collapsible,
} as Meta<typeof Collapsible>;

const Template: StoryFn<typeof Collapsible> = (args) => <Collapsible {...args}>Inner content</Collapsible>;

export const Default = Template.bind({});
Default.args = {
  label: "Optional",
};

export const WithError = Template.bind({});
WithError.args = {
  label: "Optional",
  showErrorIndicator: true,
};

export const Footer = Template.bind({});
Footer.args = {
  label: "Optional",
  type: "footer",
};

export const Section = Template.bind({});
Section.args = {
  label: "Optional section",
  type: "section",
};
