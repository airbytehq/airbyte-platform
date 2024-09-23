import { StoryObj } from "@storybook/react";

import { BorderedTiles, BorderedTile } from "components/ui/BorderedTiles";

import { Heading } from "../Heading";
import { Text } from "../Text";

export default {
  title: "ui/BorderedTiles",
  component: BorderedTiles,
  argTypes: {},
} as StoryObj<typeof BorderedTiles>;

export const Default: StoryObj<typeof BorderedTiles> = {
  args: {},
  render: (args) => (
    <BorderedTiles {...args}>
      <BorderedTile>
        <Heading as="h2">Box 1</Heading>
        <Text>Lorem ipsum dolor </Text>
      </BorderedTile>
      <BorderedTile>
        <Heading as="h2">Box 2</Heading>
        <Text>Lorem ipsum dolor </Text>
      </BorderedTile>
      <BorderedTile>
        <Heading as="h2">Box 3</Heading>
        <Text>Lorem ipsum dolor </Text>
      </BorderedTile>
      <BorderedTile>
        <Heading as="h2">Box 4</Heading>
        <Text>Lorem ipsum dolor </Text>
      </BorderedTile>
    </BorderedTiles>
  ),
};
