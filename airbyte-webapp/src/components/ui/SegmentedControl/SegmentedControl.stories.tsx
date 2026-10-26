import { action } from "@storybook/addon-actions";
import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import { SegmentedControl, SegmentedControlProps } from "./SegmentedControl";

export default {
  title: "Ui/SegmentedControl",
  component: SegmentedControl,
  argTypes: {
    isDisabled: { control: "boolean" },
    hasError: { control: "boolean" },
  },
} as Meta<typeof SegmentedControl>;

const SegmentedControlWithState: StoryFn<SegmentedControlProps<string>> = ({
  selectedValue: initial,
  ...props
}: SegmentedControlProps<string>) => {
  const [selectedValue, setSelectedValue] = useState(initial);

  const handleSelect = (value: string) => {
    action("Option selected")(value);
    setSelectedValue(value);
  };

  return (
    <div style={{ width: 580 }}>
      <SegmentedControl<string> {...props} selectedValue={selectedValue} onSelect={handleSelect} />
    </div>
  );
};

export const TwoOptions = SegmentedControlWithState.bind({});
TwoOptions.args = {
  "aria-label": "Authentication method",
  options: [
    { label: "OAuth", value: "oauth" },
    { label: "Service account key", value: "service_account" },
  ],
  selectedValue: "oauth",
};

export const ThreeOptions = SegmentedControlWithState.bind({});
ThreeOptions.args = {
  "aria-label": "Access",
  options: [
    { label: "Public bucket", value: "public" },
    { label: "Access key", value: "access_key" },
    { label: "IAM role", value: "iam_role" },
  ],
  selectedValue: "public",
};

export const LongLabels = SegmentedControlWithState.bind({});
LongLabels.args = {
  "aria-label": "Authentication",
  options: [
    { label: "Authenticate via Facebook Marketing (Oauth)", value: "oauth" },
    { label: "Service Account Key Authentication", value: "service_account" },
    { label: "No Authentication / Default AWS Credentials", value: "none" },
    { label: "IAM role", value: "iam_role" },
  ],
  selectedValue: "oauth",
};

export const NoSelection = SegmentedControlWithState.bind({});
NoSelection.args = {
  ...ThreeOptions.args,
  selectedValue: undefined,
};

export const Disabled = SegmentedControlWithState.bind({});
Disabled.args = {
  ...ThreeOptions.args,
  isDisabled: true,
};

export const WithError = SegmentedControlWithState.bind({});
WithError.args = {
  ...ThreeOptions.args,
  hasError: true,
};
