import { Meta, StoryObj } from "@storybook/react";
import { useState } from "react";

import { Option } from "components/ui/ListBox/Option";

import { MultiSelectTags, MultiSelectTagsProps } from "./MultiSelectTags";

const meta: Meta<typeof MultiSelectTags> = {
  title: "ui/MultiSelectTags",
  component: MultiSelectTags,
  argTypes: {
    selectedValues: {
      table: {
        disable: true,
      },
    },
    onSelectValues: {
      table: {
        disable: true,
      },
    },
    options: {
      table: {
        disable: true,
      },
    },
  },
};

export default meta;

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
  {
    label: "six",
    value: 6,
  },
  {
    label: "seven",
    value: 7,
  },
  {
    label: "eight",
    value: 8,
  },
  {
    label: "nine",
    value: 9,
  },
  {
    label: "ten",
    value: 10,
  },
  {
    label: "eleven",
    value: 11,
  },
  {
    label: "twelve",
    value: 12,
  },
  {
    label: "thirteen",
    value: 13,
  },
  {
    label: "fourteen",
    value: 14,
  },
  {
    label: "fifteen",
    value: 15,
  },
  {
    label: "sixteen",
    value: 16,
  },
  {
    label: "seventeen",
    value: 17,
  },
  {
    label: "eighteen",
    value: 18,
  },
  {
    label: "nineteen",
    value: 19,
  },
  {
    label: "twenty",
    value: 20,
  },
];

export const Default: StoryObj<typeof MultiSelectTags<number>> = {
  args: {
    disabled: false,
    selectedValues: undefined,
  },
  render: (args) => <MultiSelectTagsExample {...args} />,
};

const MultiSelectTagsExample = (args: MultiSelectTagsProps<number>) => {
  const [selectedValues, setSelectedValues] = useState<number[]>([]);
  return (
    <MultiSelectTags
      {...args}
      selectedValues={selectedValues}
      onSelectValues={setSelectedValues}
      options={exampleOptions}
    />
  );
};
