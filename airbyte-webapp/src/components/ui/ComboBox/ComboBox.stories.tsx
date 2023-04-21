import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import { ComboBox, ComboBoxProps } from "./ComboBox";

export default {
  title: "Ui/ComboBox",
  component: ComboBox,
} as Meta<typeof ComboBox>;

const Template: StoryFn<typeof ComboBox> = (args: Omit<ComboBoxProps, "onChange">) => {
  const [selectedValue, setSelectedValue] = useState(args.value);
  return <ComboBox {...args} value={selectedValue} onChange={setSelectedValue} />;
};

export const Primary = Template.bind({});
Primary.args = {
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
