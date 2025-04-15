import { ColumnDef } from "@tanstack/react-table";
import { render } from "@testing-library/react";

import { TestWrapper } from "test-utils/testutils";

import { Table } from "./Table";

interface Item {
  name: string;
  value: number;
}

describe("<Table>", () => {
  it("should render the table with passed data", () => {
    const data: Item[] = [
      { name: "2017", value: 100 },
      { name: "2018", value: 300 },
      { name: "2019", value: 500 },
      { name: "2020", value: 400 },
      { name: "2021", value: 200 },
    ];

    const columns: Array<ColumnDef<Item>> = [
      {
        header: "Name",
        accessorKey: "name",
        cell: ({ getValue }) => <strong>{getValue<string>()}</strong>,
      },
      {
        header: "Value",
        accessorKey: "value",
        cell: ({ getValue }) => getValue<string>(),
      },
    ];

    const { getByText, container } = render(
      <TestWrapper>
        <Table<Item> columns={columns} data={data} />
      </TestWrapper>
    );
    expect(getByText(/2019/)).toBeInTheDocument();
    expect(getByText(/500/)).toBeInTheDocument();

    expect(container.querySelectorAll("thead tr")).toHaveLength(1);
    expect(container.querySelectorAll("tbody tr")).toHaveLength(data.length);
  });
});
