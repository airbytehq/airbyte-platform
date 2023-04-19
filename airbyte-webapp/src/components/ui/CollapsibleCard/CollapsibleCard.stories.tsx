import { Meta, StoryFn } from "@storybook/react";

import { CollapsibleCard } from "./CollapsibleCard";

export default {
  title: "UI/CollapsibleCard",
  component: CollapsibleCard,
} as Meta<typeof CollapsibleCard>;

const Template: StoryFn<typeof CollapsibleCard> = (args) => <CollapsibleCard {...args} />;

export const Primary = Template.bind({});
Primary.args = {
  title: "Card Title",
  children: "The collapsible content goes here.",
  collapsible: true,
  defaultCollapsedState: true,
};

export const WithPreviewInfo = Template.bind({});
WithPreviewInfo.args = {
  title: "Card Title",
  children: "The collapsed content goes here.",
  collapsible: true,
  defaultCollapsedState: true,
  collapsedPreviewInfo: "The preview info is only visible when the card is collapsed.",
};
