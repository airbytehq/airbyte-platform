import { StoryObj } from "@storybook/react";
import { useState } from "react";

import { DatePicker, DatePickerProps } from "./DatePicker";

export default {
  title: "ui/DatePicker",
  component: DatePicker,
  argTypes: {
    onChange: { action: "changed" },
  },
} as StoryObj<typeof DatePicker>;

// Note: storybook
const Template: React.FC<DatePickerProps> = (args) => {
  const [value, setValue] = useState("");

  return (
    <DatePicker
      {...args}
      value={value}
      onChange={(value) => {
        args.onChange(value);
        setValue(value);
      }}
      key="static"
    />
  );
};

export const YearMonthDay: StoryObj<typeof DatePicker> = {
  args: {
    placeholder: "YYYY-MM-DD",
  },
  render: (args) => <Template {...args} />,
};

export const UtcTimestamp: StoryObj<typeof DatePicker> = {
  args: {
    placeholder: "YYYY-MM-DDTHH:mm:ssZ",
    withTime: true,
  },
  render: (args) => <Template {...args} />,
};

export const UtcTimestampWithMillieconds: StoryObj<typeof DatePicker> = {
  args: {
    placeholder: "YYYY-MM-DDTHH:mm:ss.SSSZ",
    withTime: true,
    withPrecision: "milliseconds",
  },
  render: (args) => <Template {...args} />,
};

export const UtcTimestampWithMicroseconds: StoryObj<typeof DatePicker> = {
  args: {
    placeholder: "YYYY-MM-DDTHH:mm:ss.SSSSSSZ",
    withTime: true,
    withPrecision: "microseconds",
  },
  render: (args) => <Template {...args} />,
};
