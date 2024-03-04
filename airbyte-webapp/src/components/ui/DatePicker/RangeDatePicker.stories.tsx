import { StoryObj } from "@storybook/react";
import { useState } from "react";

import { RangeDatePicker, RangeDatePickerProps } from "./RangeDatePicker";

export default {
  title: "ui/RangeDatePicker",
  component: RangeDatePicker,
  argTypes: {
    onChange: { action: "changed" },
  },
} as StoryObj<typeof RangeDatePicker>;

const Template: React.FC<RangeDatePickerProps> = (args) => {
  const [value, setValue] = useState<[string, string]>(["", ""]);

  return (
    <RangeDatePicker
      {...args}
      value={value}
      onChange={(value: [string, string]) => {
        args.onChange(value);
        setValue(value);
      }}
    />
  );
};

export const Base: StoryObj<typeof RangeDatePicker> = {
  args: {},
  render: (args) => <Template {...args} />,
};

export const WitDateFormat: StoryObj<typeof RangeDatePicker> = {
  args: {
    dateFormat: "YYYY-MM-DD",
  },
  render: (args) => <Template {...args} />,
};
