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
