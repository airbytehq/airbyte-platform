/* eslint-disable no-only-tests/no-only-tests */
import { ColumnDef } from "@tanstack/react-table";
import { renderHook } from "@testing-library/react";

import { useSyncCatalogReactTable } from "./useSyncCatalogReactTable";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

describe("useSyncCatalogReactTable", () => {
  const mockColumns: Array<ColumnDef<SyncCatalogUIModel>> = [
    {
      id: "stream.name",
      accessorKey: "name",
      header: "Name",
      enableGlobalFilter: true,
    },
  ];

  const mockData: SyncCatalogUIModel[] = [
    {
      name: "namespace1",
      rowType: "namespace",
      subRows: [
        {
          name: "stream_1",
          rowType: "stream",
          subRows: [
            {
              name: "field1",
              rowType: "field",
              subRows: [
                {
                  name: "nested_field1_1",
                  rowType: "nestedField",
                  subRows: [],
                },
                {
                  name: "nested_field1_2",
                  rowType: "nestedField",
                  subRows: [],
                },
              ],
            },
            {
              name: "field2",
              rowType: "field",
              subRows: [],
            },
          ],
        },
        {
          name: "test_stream",
          rowType: "stream",
          subRows: [
            {
              name: "test_field",
              rowType: "field",
              subRows: [
                {
                  name: "test_nested_field",
                  rowType: "nestedField",
                  subRows: [],
                },
              ],
            },
          ],
        },
      ],
    },
    {
      name: "namespace2",
      rowType: "namespace",
      subRows: [
        {
          name: "stream_2",
          rowType: "stream",
          subRows: [
            {
              name: "field3",
              rowType: "field",
              subRows: [
                {
                  name: "nested_field2_1",
                  rowType: "nestedField",
                  subRows: [],
                },
              ],
            },
          ],
        },
      ],
    },
    {
      name: "namespace3",
      rowType: "namespace",
      subRows: [
        {
          name: "stream_3",
          rowType: "stream",
          subRows: [
            {
              name: "stream_3_field_3",
              rowType: "field",
              subRows: [
                {
                  name: "stream_3_field_3_nested_field4",
                  rowType: "nestedField",
                  subRows: [],
                },
              ],
            },
          ],
        },
      ],
    },
  ];

  // important to callculate the initial expanded state based on the mockData,
  // otherwise we wll get more rows than expected
  const initialExpandedState = Object.fromEntries(mockData.map((_, index) => [index, true]));

  const defaultProps = {
    columns: mockColumns,
    data: mockData,
    expanded: initialExpandedState,
    setExpanded: jest.fn(),
    globalFilter: "",
    columnFilters: [],
    setColumnFilters: jest.fn(),
    showHashing: false,
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("global filter", () => {
    it("should return all rows when global filter is empty", () => {
      const { result } = renderHook(() =>
        useSyncCatalogReactTable({
          ...defaultProps,
          globalFilter: "",
        })
      );

      const rows = result.current.getRowModel().rows;
      // since we calculate the initial expanded state based on the mockData,
      // we expect 7 rows: 3 namespaces and 4 streams
      expect(rows.length).toBe(7);
    });

    it("should filter rows based on global filter value - namespace name", () => {
      const { result } = renderHook(() =>
        useSyncCatalogReactTable({
          ...defaultProps,
          globalFilter: "namespace2",
        })
      );

      const rows = result.current.getRowModel().rows;

      expect(rows[0].original.name).toBe("namespace2");
      expect(rows.length).toBe(1);
    });

    it("should filter rows based on global filter value - stream name", () => {
      const { result } = renderHook(() =>
        useSyncCatalogReactTable({
          ...defaultProps,
          globalFilter: "stream_2",
        })
      );

      const rows = result.current.getRowModel().rows;

      expect(rows[0].original.name).toBe("namespace2");
      expect(rows[1].original.name).toBe("stream_2");
      expect(rows.length).toBe(2);
    });

    it("should filter rows based on global filter value - field name", () => {
      const { result } = renderHook(() =>
        useSyncCatalogReactTable({
          ...defaultProps,
          globalFilter: "field3",
        })
      );

      const rows = result.current.getRowModel().rows;

      expect(rows[0].original.name).toBe("namespace2");
      expect(rows[1].original.name).toBe("stream_2");
      expect(rows[2].original.name).toBe("field3");
      expect(rows.length).toBe(3);
    });

    it("should filter rows based on global filter value - nested field name", () => {
      const { result } = renderHook(() =>
        useSyncCatalogReactTable({
          ...defaultProps,
          globalFilter: "nested_field2_1",
        })
      );

      const rows = result.current.getRowModel().rows;

      expect(rows[0].original.name).toBe("namespace2");
      expect(rows[1].original.name).toBe("stream_2");
      expect(rows[2].original.name).toBe("field3");
      expect(rows[3].original.name).toBe("nested_field2_1");
      expect(rows.length).toBe(4);
    });

    it("should return empty results when no rows match the filter", () => {
      const { result } = renderHook(() =>
        useSyncCatalogReactTable({
          ...defaultProps,
          globalFilter: "non_existent_name",
        })
      );

      const rows = result.current.getRowModel().rows;
      expect(rows.length).toBe(0);
    });

    it("should show all fields and nested fields when global filter is applied after the row expansion", () => {
      const { result, rerender } = renderHook((props) => useSyncCatalogReactTable(props), {
        initialProps: {
          ...defaultProps,
          globalFilter: "stream_2",
        },
      });

      const rows = result.current.getRowModel().rows;
      expect(rows[0].original.name).toBe("namespace2");
      expect(rows[1].original.name).toBe("stream_2");
      expect(rows.length).toBe(2);

      // rerender with expanded "stream_2" state
      rerender({
        ...defaultProps,
        globalFilter: "stream_2",
        expanded: { 0: true, 1: true, "1.0": true, "1.0.0": true, "1.0.0.0": true },
      });

      const updatedRows = result.current.getRowModel().rows;

      expect(updatedRows[0].original.name).toBe("namespace2");
      expect(updatedRows[1].original.name).toBe("stream_2"); // expanded stream
      expect(updatedRows[2].original.name).toBe("field3");
      expect(updatedRows[3].original.name).toBe("nested_field2_1");
      expect(updatedRows.length).toBe(4);
    });

    it("should show only the matched streams when globalFilterMaxDepth is set to stream level", () => {
      const { result } = renderHook(() =>
        useSyncCatalogReactTable({
          ...defaultProps,
          globalFilter: "stream_3",
          globalFilterMaxDepth: 1,
        })
      );

      const rows = result.current.getRowModel().rows;
      console.log(rows.map((row) => ({ id: row.id, name: row.original.name })));

      expect(rows[0].original.name).toBe("namespace3");
      expect(rows[1].original.name).toBe("stream_3");
      expect(rows.length).toBe(2);
    });

    it("should show all fields and nested fields when globalFilterMaxDepth is set to stream level but stream is expanded", () => {
      const { result } = renderHook(() =>
        useSyncCatalogReactTable({
          ...defaultProps,
          globalFilter: "stream_3",
          globalFilterMaxDepth: 1,
          expanded: { 2: true, "2.0": true, "2.0.0": true, "2.0.0.0": true },
        })
      );

      const rows = result.current.getRowModel().rows;
      expect(rows[0].original.name).toBe("namespace3");
      expect(rows[1].original.name).toBe("stream_3");
      expect(rows[2].original.name).toBe("stream_3_field_3");
      expect(rows[3].original.name).toBe("stream_3_field_3_nested_field4");
      expect(rows.length).toBe(4);
    });
  });
});
