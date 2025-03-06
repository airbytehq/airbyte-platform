import { Meta } from "@storybook/react";
import { useState } from "react";

import { Option } from "components/ui/ListBox/Option";

import { MultiSelect, MultiSelectProps } from "./MultiSelect";

const meta: Meta<typeof MultiSelect> = {
  title: "ui/MultiSelect",
  component: MultiSelect,
  argTypes: {
    label: {
      control: {
        type: "text",
      },
    },
    options: { table: { disable: true } },
    selectedValues: { table: { disable: true } },
    onSelectValues: { table: { disable: true } },
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
    disabled: true,
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

const ControlledMultiSelect = (args: MultiSelectProps<number>) => {
  const [selectedValues, setSelectedValues] = useState<number[]>([]);
  return (
    <div style={{ width: 120 }}>
      <MultiSelect
        {...args}
        selectedValues={selectedValues}
        onSelectValues={setSelectedValues}
        options={exampleOptions}
      />
    </div>
  );
};

export const Default = {
  args: { label: "Numbers" },
  render: (args: MultiSelectProps<number>) => <ControlledMultiSelect {...args} />,
};
