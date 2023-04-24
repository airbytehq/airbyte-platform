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
