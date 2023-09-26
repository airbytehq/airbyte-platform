import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import { PillListBox } from "./PillListBox";
import { ListBoxProps } from "../ListBox";

export default {
  title: "UI/PillListBox",
  component: PillListBox,
} as Meta<typeof PillListBox>;

const Template: StoryFn<typeof PillListBox> = <T,>(args: Omit<ListBoxProps<T>, "onSelect">) => {
  const [selectedValue, setSelectedValue] = useState(args.selectedValue);
  return <PillListBox {...args} selectedValue={selectedValue} onSelect={setSelectedValue} />;
};

const options = [
  {
    value: "id",
    label: "id",
  },
  {
    value: "first_name",
    label: "first_name",
  },
  {
    value: "last_name",
    label: "last_name",
  },
  {
    value: "email",
    label: "email",
  },
  {
    value: "company",
    label: "company",
  },
];

export const Primary = Template.bind({});
Primary.args = {
  options,
  selectedValue: "email",
};
