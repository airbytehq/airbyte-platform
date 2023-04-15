import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import { SecretTextArea } from "./SecretTextArea";

export default {
  title: "UI/SecretTextArea",
  component: SecretTextArea,
} as Meta<typeof SecretTextArea>;

export const Primary: StoryFn<typeof SecretTextArea> = (args) => {
  const [value, setValue] = useState(args.value);
  return <SecretTextArea {...args} value={value} onChange={(e) => setValue(e.target.value)} />;
};
Primary.args = {
  value: "testing",
  rows: 3,
};

export const WithUpload: StoryFn<typeof SecretTextArea> = (args) => {
  const [value, setValue] = useState("testing");
  return <SecretTextArea {...args} value={value} onChange={(e) => setValue(e.target.value)} onUpload={setValue} />;
};
