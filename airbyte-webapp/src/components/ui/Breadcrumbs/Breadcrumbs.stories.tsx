import { Meta, StoryFn } from "@storybook/react";

import { Breadcrumbs, BreadcrumbsDataItem } from "./Breadcrumbs";

export default {
  title: "UI/Breadcrumbs",
  component: Breadcrumbs,
  argTypes: {},
} as Meta<typeof Breadcrumbs>;

const Template: StoryFn<typeof Breadcrumbs> = (args) => {
  return <Breadcrumbs data={args.data} />;
};

const data: BreadcrumbsDataItem[] = [
  {
    label: "Workspace",
    to: "/workspace",
  },
  {
    label: "Source",
    to: "/workspace/source",
  },
  {
    label: "Settings",
  },
];

export const Primary = Template.bind({});
Primary.args = {
  data,
};
