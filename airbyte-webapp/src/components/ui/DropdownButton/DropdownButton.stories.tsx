import { Meta, StoryFn } from "@storybook/react";

import { DropdownButton, DropdownButtonProps } from "./DropdownButton";
import { Icon } from "../Icon";

export default {
  title: "UI/DropdownButton",
  component: DropdownButton,
  argTypes: {
    backgroundColor: { control: "color" },
  },
} as Meta<typeof DropdownButton>;

const Template: StoryFn<typeof DropdownButton> = (args) => <DropdownButton {...args} />;

const defaultArgs: DropdownButtonProps = {
  variant: "primary",
  children: "Primary",
  disabled: false,
  onClick: () => alert("Main button clicked"),
  dropdown: {
    options: [
      {
        icon: <Icon size="sm" type="lightbulb" />,
        displayName: "First option",
      },
      {
        icon: <Icon size="sm" type="certified" />,
        displayName: "Second option",
      },
    ],
    textSize: "md",
    onSelect: (option) => alert(`Selected option: ${option.displayName}`),
  },
};

export const Default = Template.bind({});
Default.args = defaultArgs;

export const Secondary = Template.bind({});
Secondary.args = {
  ...defaultArgs,
  variant: "secondary",
  children: "Secondary",
};

export const Disabled = Template.bind({});
Disabled.args = {
  ...defaultArgs,
  children: "Disabled",
  disabled: true,
};

export const DisabledOption = Template.bind({});
DisabledOption.args = {
  ...defaultArgs,
  children: "Button",
  dropdown: {
    ...defaultArgs.dropdown,
    options: [
      ...defaultArgs.dropdown.options,
      {
        icon: <Icon size="sm" type="errorOutline" />,
        displayName: "Third option",
        disabled: true,
      },
    ],
  },
};
