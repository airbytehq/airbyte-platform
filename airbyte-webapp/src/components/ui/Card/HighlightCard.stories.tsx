import { Meta, StoryFn } from "@storybook/react";

import { HighlightCard } from "./HighlightCard";

export default {
  title: "Ui/HighlightCard",
  component: HighlightCard,
} as Meta<typeof HighlightCard>;

const Template: StoryFn<typeof HighlightCard> = (args) => <HighlightCard {...args}>Inner content</HighlightCard>;

export const Default = Template.bind({});
Default.args = {};
