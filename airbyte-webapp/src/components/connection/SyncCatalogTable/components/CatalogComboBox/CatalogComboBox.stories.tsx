import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import {
  CatalogComboBox,
  CatalogComboBoxProps,
  MultiCatalogComboBox,
  MultiCatalogComboBoxProps,
} from "./CatalogComboBox";

export default {
  title: "Ui/CatalogComboBox",
  component: CatalogComboBox,
  argTypes: {
    error: { control: "boolean" },
  },
} as Meta<typeof CatalogComboBox>;

const SingleValueTemplate: StoryFn<typeof CatalogComboBox> = (args: Omit<CatalogComboBoxProps, "onChange">) => {
  const [selectedValue, setSelectedValue] = useState(args.value);
  return <CatalogComboBox {...args} value={selectedValue} onChange={setSelectedValue} />;
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
      value: "mongodb",
      description: "the third value",
    },
  ],
  value: "",
};

const MultiValueTemplate: StoryFn<typeof MultiCatalogComboBox> = (
  args: Omit<MultiCatalogComboBoxProps, "onChange">
) => {
  const [value, setValue] = useState(args.value);
  return (
    <div style={{ border: "1px solid red", padding: "20px", resize: "horizontal" }}>
      <MultiCatalogComboBox {...args} value={value} onChange={setValue} />
    </div>
  );
};

export const MultiValue = MultiValueTemplate.bind({});
MultiValue.args = {
  options: [
    { value: "john doe", description: "description for John Doe" },
    { value: "jane smith", description: "description for Jane Smith" },
    { value: "alice johnson", description: "description for Alice Johnson" },
    { value: "bob brown", description: "description for Bob Brown" },
    { value: "charlie davis", description: "description for Charlie Davis" },
    { value: "diana evans", description: "description for Diana Evans" },
    { value: "edward frank", description: "description for Edward Frank" },
    { value: "fiona green", description: "description for Fiona Green" },
    { value: "george harris", description: "description for George Harris" },
    { value: "hannah ivan", description: "description for Hannah Ivan" },
    { value: "ian jackson", description: "description for Ian Jackson" },
    { value: "julia kent", description: "description for Julia Kent" },
    { value: "kevin lee", description: "description for Kevin Lee" },
    { value: "linda martin", description: "description for Linda Martin" },
    { value: "michael nelson", description: "description for Michael Nelson" },
    { value: "nina owen", description: "description for Nina Owen" },
    { value: "oliver perez", description: "description for Oliver Perez" },
    { value: "paula quinn", description: "description for Paula Quinn" },
    { value: "quentin ross", description: "description for Quentin Ross" },
    { value: "rachel smith", description: "description for Rachel Smith" },
    { value: "steve taylor", description: "description for Steve Taylor" },
    { value: "tina usher", description: "description for Tina Usher" },
    { value: "victor vance", description: "description for Victor Vance" },
    { value: "wendy white", description: "description for Wendy White" },
    { value: "xander young", description: "description for Xander Young" },
    { value: "yvonne zane", description: "description for Yvonne Zane" },
    { value: "zachary adams", description: "description for Zachary Adams" },
    { value: "amelia baker", description: "description for Amelia Baker" },
    { value: "brandon clark", description: "description for Brandon Clark" },
    { value: "chloe dixon", description: "description for Chloe Dixon" },
  ],
  value: [],
};
