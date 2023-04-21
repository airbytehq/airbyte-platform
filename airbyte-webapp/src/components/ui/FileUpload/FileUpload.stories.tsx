import { Meta, StoryFn } from "@storybook/react";

import { FileUpload } from "./FileUpload";

export default {
  title: "UI/FileUpload",
  component: FileUpload,
} as Meta<typeof FileUpload>;

const Template: StoryFn<typeof FileUpload> = (args) => (
  <FileUpload {...args} onUpload={(content) => alert(`Uploaded file with content:\n\n${content}`)} />
);

export const Primary = Template.bind({});
Primary.args = {};
