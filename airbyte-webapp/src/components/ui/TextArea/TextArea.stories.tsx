import { StoryFn, Meta } from "@storybook/react";
import { useState } from "react";

import { TextArea } from "./TextArea";

export default {
  title: "Ui/TextArea",
  component: TextArea,
} as Meta<typeof TextArea>;

const Template: StoryFn<typeof TextArea> = (args) => <TextArea {...args} />;

export const Primary = Template.bind({});
Primary.args = {
  placeholder: "Enter text here...",
  rows: 3,
};

export const WithUpload: StoryFn<typeof TextArea> = (args) => {
  const [value, setValue] = useState("");
  return <TextArea {...args} value={value} onUpload={setValue} onChange={(e) => setValue(e.target.value)} />;
};
WithUpload.args = {
  placeholder: "Enter text here or upload file",
  rows: 3,
};
