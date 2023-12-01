import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import { ListBox, ListBoxProps } from "./ListBox";

export default {
  title: "Ui/ListBox",
  component: ListBox,
  argTypes: {
    placement: {
      options: [
        "top",
        "top-start",
        "top-end",
        "right",
        "right-start",
        "right-end",
        "bottom",
        "bottom-start",
        "bottom-end",
        "left",
        "left-start",
        "left-end",
      ],
      control: { type: "radio" },
    },
  },
} as Meta<typeof ListBox>;

const Template: StoryFn<typeof ListBox> = <T,>(args: Omit<ListBoxProps<T>, "onSelect">) => {
  const [selectedValue, setSelectedValue] = useState(args.selectedValue);
  return <ListBox {...args} selectedValue={selectedValue} onSelect={setSelectedValue} />;
};

const options = [
  {
    label: "one",
    value: 1,
    icon: <Icon type="pencil" />,
  },
  {
    label: "two",
    value: 2,
  },
  {
    label: "three",
    value: 3,
  },
];

export const Primary = Template.bind({});
Primary.args = {
  options,
  selectedValue: 1,
};

export const Placement = Template.bind({});
Placement.args = {
  options,
  adaptiveWidth: false,
};
Placement.decorators = [
  (Story: StoryFn) => (
    <FlexContainer
      alignItems="center"
      justifyContent="center"
      style={{ width: 500, height: 500, border: "1px solid #494961" }}
    >
      <Story />
    </FlexContainer>
  ),
];

export const ValueAsObject = Template.bind({});
ValueAsObject.args = {
  options: [
    {
      label: "Basic",
      value: { scheduleType: "basic" },
    },
    {
      label: "Manual",
      value: { scheduleType: "manual" },
    },
    {
      label: "Cron",
      value: { scheduleType: "cron" },
    },
  ],
  selectedValue: { scheduleType: "basic" },
};
