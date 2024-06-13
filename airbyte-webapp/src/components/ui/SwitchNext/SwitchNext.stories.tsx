import { action } from "@storybook/addon-actions";
import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import { SwitchNext, SwitchNextProps } from "./SwitchNext";

export default {
  title: "Ui/SwitchNext",
  component: SwitchNext,
  argTypes: {
    checked: { control: "boolean" },
  },
} as Meta<typeof SwitchNext>;

const SwitchNextWithState: StoryFn<typeof SwitchNext> = ({ checked: initial = false, ...props }: SwitchNextProps) => {
  const [checked, setChecked] = useState<boolean>(initial);
  const [loading, setLoading] = useState<boolean>(false);

  const handleChange = (checked: boolean) => {
    action("Switch toggled")(checked);
    setLoading(true);
    setTimeout(() => {
      setChecked(checked);
      setLoading(false);
    }, 1500);
  };

  return <SwitchNext {...props} checked={checked} loading={loading} onChange={handleChange} />;
};

export const Primary = SwitchNextWithState.bind({});
Primary.args = {};
