import { faEdit } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { Meta, StoryObj } from "@storybook/react";

import { ListBox } from "./ListBox";

export default {
  title: "Ui/ListBox",
  component: ListBox,
} as Meta<typeof ListBox>;

export const Primary: StoryObj<typeof ListBox> = {
  args: {
    options: [
      {
        label: "one",
        value: 1,
        icon: <FontAwesomeIcon icon={faEdit} />,
      },
      {
        label: "two",
        value: 2,
      },
      {
        label: "three",
        value: 3,
      },
    ],
  },
};
