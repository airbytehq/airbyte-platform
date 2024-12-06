import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import { ComboBox, ComboBoxProps, MultiComboBox, MultiComboBoxProps } from "./ComboBox";
import { FlexContainer } from "../Flex";
import { Icon } from "../Icon";
import { Text } from "../Text";

export default {
  title: "Ui/ComboBox",
  component: ComboBox,
} as Meta<typeof ComboBox>;

const SingleValueTemplate: StoryFn<typeof ComboBox> = (args: Omit<ComboBoxProps, "onChange">) => {
  const [selectedValue, setSelectedValue] = useState(args.value);
  return (
    <FlexContainer direction="column" gap="lg">
      <ComboBox {...args} value={selectedValue} onChange={setSelectedValue} />
      <Text>Selected value: {selectedValue}</Text>
    </FlexContainer>
  );
};

export const SingleValue = SingleValueTemplate.bind({});
SingleValue.args = {
  options: [
    {
      value: "postgres",
      description: "the first value",
    },
    {
      value: "mysql",
      description: "the second value",
    },
    {
      value: "mssql",
      description: "the third value",
    },
  ],
  value: "",
  error: false,
  filterOptions: true,
  allowCustomValue: true,
};

export const WithDifferingLabels = SingleValueTemplate.bind({});
WithDifferingLabels.args = {
  options: [
    {
      value: "a1b2c3",
      label: "small",
    },
    {
      value: "d4e5f6",
      label: "medium",
    },
    {
      value: "g7h8i9",
      label: "large",
    },
  ],
  value: "",
  error: false,
  filterOptions: true,
  allowCustomValue: false,
};

export const WithOptionSections = SingleValueTemplate.bind({});
WithOptionSections.args = {
  options: [
    {
      sectionTitle: "Databases",
      innerOptions: [
        {
          value: "postgres",
        },
        {
          value: "mysql",
        },
        {
          value: "mssql",
        },
      ],
    },
    {
      sectionTitle: "APIs",
      innerOptions: [
        {
          value: "slack",
        },
        {
          value: "salesforce",
        },
      ],
    },
  ],
  value: "",
  error: false,
  filterOptions: true,
  allowCustomValue: false,
};

export const WithIcons = SingleValueTemplate.bind({});
WithIcons.args = {
  options: [
    {
      value: "success",
      iconLeft: <Icon type="statusSuccess" size="sm" color="success" />,
    },
    {
      value: "warning",
      iconLeft: <Icon type="statusWarning" size="sm" color="warning" />,
    },
    {
      value: "error",
      iconLeft: <Icon type="statusError" size="sm" color="error" />,
    },
    {
      value: "error",
      iconRight: <Icon type="aiStars" size="sm" color="magic" />,
    },
  ],
  value: "",
  error: false,
  filterOptions: true,
  allowCustomValue: false,
};

const MultiValueTemplate: StoryFn<typeof MultiComboBox> = (args: Omit<MultiComboBoxProps, "onChange">) => {
  const [value, setValue] = useState(args.value);
  return <MultiComboBox {...args} value={value} onChange={setValue} />;
};

export const MultiValue = MultiValueTemplate.bind({});
MultiValue.args = {
  options: [
    {
      value: "postgres",
      description: "the first value",
    },
    {
      value: "mysql",
      description: "the second value",
    },
    {
      value: "mssql",
      description: "the third value",
    },
  ],
  value: ["gcs"],
  error: false,
};
