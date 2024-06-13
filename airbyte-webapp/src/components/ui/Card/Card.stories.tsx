import { ComponentStory, ComponentMeta } from "@storybook/react";

import { Card } from "./Card";

export default {
  title: "Ui/Card",
  component: Card,
} as ComponentMeta<typeof Card>;

const Template: ComponentStory<typeof Card> = (args) => <Card {...args} />;

export const CardWithTitle = Template.bind({});
CardWithTitle.args = {
  title: "Title",
};

export const CardWithBody = Template.bind({});
CardWithBody.args = {
  children: "Body Text",
};

export const CardWithoutDefaultPadding = Template.bind({});
CardWithoutDefaultPadding.args = {
  children: "Body Text",
  noPadding: true,
};

export const CardWithTitleAndBody = Template.bind({});
CardWithTitleAndBody.args = {
  title: "Title",
  children: "Body Text",
};

export const CardWithTitleAndBottomBorder = Template.bind({});
CardWithTitleAndBottomBorder.args = {
  title: "Title",
  children: "Body Text",
  titleWithBottomBorder: true,
};

export const Collapsible = Template.bind({});
Collapsible.args = {
  title: "Title",
  children: "The collapsible content goes here.",
  collapsible: true,
  defaultCollapsedState: true,
};

export const CollapsibleWithPreviewInfo = Template.bind({});
CollapsibleWithPreviewInfo.args = {
  title: "Title",
  children: (
    <div>
      Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore
      magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
      consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
      Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
    </div>
  ),
  collapsible: true,
  defaultCollapsedState: true,
  collapsedPreviewInfo: (
    <div style={{ border: "1px solid red", padding: 10 }}>
      The preview info is only visible when the card is collapsed. It's also clickable and will toggle the card's state.
    </div>
  ),
};

export const CardWithHelpText = Template.bind({});
CardWithHelpText.args = {
  title: "Title",
  children: "Card content here",
  helpText: "This is helpful text",
};

export const CardWithHelpDescription = Template.bind({});
CardWithHelpDescription.args = {
  title: "Title",
  children: "Card content here",
  description: "This is descriptive text",
};
