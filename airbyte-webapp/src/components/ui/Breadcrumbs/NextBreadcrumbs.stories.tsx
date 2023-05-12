import { Meta, StoryFn } from "@storybook/react";

import { BreadcrumbsDataItem } from "./Breadcrumbs";
import { NextBreadcrumbs } from "./NextBreadcrumbs";

export default {
  title: "UI/NextBreadcrumbs",
  component: NextBreadcrumbs,
  argTypes: {},
} as Meta<typeof NextBreadcrumbs>;

const Template: StoryFn<typeof NextBreadcrumbs> = (args) => {
  return <NextBreadcrumbs data={args.data} />;
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
