import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import { ComboBox, ComboBoxProps, MultiComboBox, MultiComboBoxProps } from "./ComboBox";

export default {
  title: "Ui/ComboBox",
  component: ComboBox,
} as Meta<typeof ComboBox>;

const SingleValueTemplate: StoryFn<typeof ComboBox> = (args: Omit<ComboBoxProps, "onChange">) => {
  const [selectedValue, setSelectedValue] = useState(args.value);
  return <ComboBox {...args} value={selectedValue} onChange={setSelectedValue} />;
};

export const SingleValue = SingleValueTemplate.bind({});
SingleValue.args = {
  options: [
    {
      value: "postgres",
      description: "the first value",
    },
    {
      value: "mysql",
      description: "the second value",
    },
    {
      value: "mssql",
      description: "the third value",
    },
  ],
  value: "s",
  error: false,
};

const MultiValueTemplate: StoryFn<typeof MultiComboBox> = (args: Omit<MultiComboBoxProps, "onChange">) => {
  const [value, setValue] = useState(args.value);
  return <MultiComboBox {...args} value={value} onChange={setValue} />;
};

export const MultiValue = MultiValueTemplate.bind({});
MultiValue.args = {
  options: [
    {
      value: "postgres",
      description: "the first value",
    },
    {
      value: "mysql",
      description: "the second value",
    },
    {
      value: "mssql",
      description: "the third value",
    },
  ],
  value: ["gcs"],
  error: false,
};
