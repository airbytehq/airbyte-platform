import { faEdit } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { Meta, StoryObj, StoryFn } from "@storybook/react";

import { FlexContainer } from "components/ui/Flex";

import { ListBox } from "./ListBox";

export default {
  title: "Ui/ListBox",
  component: ListBox,
  argTypes: {
    placement: {
      options: [
        "top",
        "top-start",
        "top-end",
        "right",
        "right-start",
        "right-end",
        "bottom",
        "bottom-start",
        "bottom-end",
        "left",
        "left-start",
        "left-end",
      ],
      control: { type: "radio" },
    },
  },
} as Meta<typeof ListBox>;

type Story = StoryObj<typeof ListBox>;

const options = [
  {
    label: "one",
    value: 1,
    icon: <FontAwesomeIcon icon={faEdit} />,
  },
  {
    label: "two",
    value: 2,
  },
  {
    label: "three",
    value: 3,
  },
];

export const Primary: Story = {
  args: {
    options,
  },
};

export const Placement: Story = {
  args: {
    options,
    adaptiveWidth: false,
  },
  decorators: [
    (Story: StoryFn) => (
      <FlexContainer
        alignItems="center"
        justifyContent="center"
        style={{ width: 500, height: 500, border: "1px solid #494961" }}
      >
        <Story />
      </FlexContainer>
    ),
  ],
};
