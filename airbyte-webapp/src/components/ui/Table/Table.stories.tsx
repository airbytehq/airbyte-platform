import { Story, StoryObj } from "@storybook/react";
import { createColumnHelper } from "@tanstack/react-table";

import { Table, TableProps } from "./Table";

interface Item {
  name: string;
  value: number;
}

export default {
  title: "UI/Table",
  component: Table,
  argTypes: {},
} as StoryObj<typeof Table>;

const Template =
  <T,>(): Story<TableProps<T>> =>
  (args) => (
    <div style={{ height: "100vh" }}>
      <Table<T> {...args} />
    </div>
  );

const data: Item[] = [
  { name: "2017", value: 100 },
  { name: "2018", value: 300 },
  { name: "2019", value: 500 },
  { name: "2020", value: 400 },
  { name: "2021", value: 200 },
];

const columnHelper = createColumnHelper<Item>();

const columns = [
  columnHelper.accessor("name", {
    header: "Name",
    cell: ({ getValue }) => <strong>{getValue<string>()}</strong>,
  }),
  columnHelper.accessor("value", {
    header: "Value",
    cell: ({ getValue }) => getValue<string>(),
  }),
];

export const Primary = Template<Item>().bind({});
Primary.args = {
  data,
  columns,
};

export const PrimaryEmpty = Template<Item>().bind({});
PrimaryEmpty.args = {
  data: [],
  columns,
};

export const Virtualized = Template<Item>().bind({});
Virtualized.args = {
  data,
  columns,
  virtualized: true,
};

export const VirtualizedEmpty = Template<Item>().bind({});
VirtualizedEmpty.args = {
  data: [],
  columns,
  virtualized: true,
};

export const VirtualizedCustomEmptyPlaceholder = Template<Item>().bind({});
VirtualizedCustomEmptyPlaceholder.args = {
  data: [],
  columns,
  virtualized: true,
  customEmptyPlaceholder: <div>Custom empty placeholder</div>,
};
