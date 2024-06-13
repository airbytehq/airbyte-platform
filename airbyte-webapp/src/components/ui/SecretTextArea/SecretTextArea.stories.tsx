import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Text } from "components/ui/Text";

import { SecretTextArea } from "./SecretTextArea";

export default {
  title: "UI/SecretTextArea",
  component: SecretTextArea,
  argTypes: {
    value: { control: "text" },
    disabled: { control: "boolean" },
    light: { control: "boolean" },
    error: { control: "boolean" },
  },
} as Meta<typeof SecretTextArea>;

const Template: StoryFn<typeof SecretTextArea> = (args) => (
  <Card>
    <Text size="lg">An extremely secret text area</Text>
    <Box my="md">
      <SecretTextArea
        {...args}
        onChange={() => {
          // eslint-disable-next-line @typescript-eslint/no-empty-function
        }}
      />
    </Box>
  </Card>
);

export const Primary = Template.bind({});
Primary.args = {
  value: "testing",
  disabled: false,
  light: false,
  error: false,
};

export const WithUpload: StoryFn<typeof SecretTextArea> = (args) => {
  const [value, setValue] = useState("testing");
  return (
    <Card>
      <Text size="lg">An extremely secret text area with file upload</Text>
      <Box my="md">
        <SecretTextArea {...args} value={value} onChange={(e) => setValue(e.target.value)} onUpload={setValue} />
      </Box>
    </Card>
  );
};
