import { StoryObj } from "@storybook/react";
import { useState } from "react";

import { Option } from "./ListBox";
import { MultiListBox, MultiListBoxProps } from "./MultiListBox";

export default {
  title: "ui/MultiListBox",
  component: MultiListBox,
} as StoryObj<typeof MultiListBox>;

const exampleOptions: Array<Option<number>> = [
  {
    label: "one",
    value: 1,
  },
  {
    label: "two",
    value: 2,
  },
  {
    label: "three",
    value: 3,
  },
  {
    label: "four",
    value: 4,
  },
  {
    label: "five",
    value: 5,
  },
];

export const Default: StoryObj<typeof MultiListBox<number>> = {
  args: {
    label: "Items",
  },
  render: (args) => <MultiListBoxExample {...args} />,
};

const MultiListBoxExample = (args: MultiListBoxProps<number>) => {
  const [selectedValues, setSelectedValues] = useState<number[]>([]);
  return (
    <MultiListBox
      {...args}
      selectedValues={selectedValues}
      onSelectValues={setSelectedValues}
      options={exampleOptions}
    />
  );
};
