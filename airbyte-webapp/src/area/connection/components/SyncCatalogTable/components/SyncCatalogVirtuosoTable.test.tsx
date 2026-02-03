import { HeaderGroup, Row } from "@tanstack/react-table";
import { render, screen } from "@testing-library/react";
import { TableVirtuoso } from "react-virtuoso";

import { TestWrapper } from "test-utils/testutils";

import { SyncCatalogVirtuosoTable } from "./SyncCatalogVirtuosoTable";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

jest.mock("core/services/ui/FormModeContext", () => ({
  useFormMode: () => ({
    mode: "create",
  }),
}));

// Mock the react-virtuoso module
jest.mock("react-virtuoso", () => {
  // Create a custom mock implementation that calls fixedHeaderContent
  const TableVirtuosoMock = jest.fn((props) => {
    // Call the fixedHeaderContent function to ensure it runs
    if (props.fixedHeaderContent) {
      props.fixedHeaderContent();
    }

    // Render the EmptyPlaceholder when rows are empty
    if (props.totalCount === 0 && props.components?.EmptyPlaceholder) {
      const EmptyPlaceholder = props.components.EmptyPlaceholder;
      return (
        <div data-testid="mocked-table-virtuoso">
          <EmptyPlaceholder context={props.context} />
        </div>
      );
    }

    return <div data-testid="mocked-table-virtuoso" />;
  });

  return {
    TableVirtuoso: TableVirtuosoMock,
  };
});

describe("SyncCatalogVirtuosoTable", () => {
  const mockRows = [
    {
      id: "namespace1",
      depth: 0,
      original: {
        rowType: "namespace",
        name: "namespace1",
      },
      getVisibleCells: jest.fn().mockReturnValue([]),
    },
    {
      id: "namespace1.stream1",
      depth: 1,
      original: {
        rowType: "stream",
        name: "stream1",
        namespace: "namespace1",
        isEnabled: true,
      },
      getVisibleCells: jest.fn().mockReturnValue([]),
    },
  ] as unknown as Array<Row<SyncCatalogUIModel>>;

  const mockHeaderGroups = [
    {
      id: "group1",
      headers: [
        {
          id: "header1",
          column: {
            columnDef: {
              header: "Header 1",
              meta: {
                thClassName: "test-class",
              },
            },
          },
          getContext: jest.fn().mockReturnValue({}),
        },
      ],
    },
  ] as unknown as Array<HeaderGroup<SyncCatalogUIModel>>;

  const mockGetHeaderGroups = jest.fn().mockReturnValue(mockHeaderGroups);

  const mockGetState = jest.fn().mockReturnValue({
    globalFilter: "",
  });

  const defaultProps = {
    rows: mockRows,
    getHeaderGroups: mockGetHeaderGroups,
    getState: mockGetState,
    initialTopMostItemIndex: undefined,
    stickyRowIndex: 0,
    setStickyRowIndex: jest.fn(),
    columnFilters: [],
    stickyIndexes: [0],
    expanded: {},
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should render TableVirtuoso with the correct props", () => {
    render(
      <TestWrapper>
        <SyncCatalogVirtuosoTable {...defaultProps} />
      </TestWrapper>
    );

    expect(screen.getByTestId("mocked-table-virtuoso")).toBeInTheDocument();

    // Verify that TableVirtuoso was called with the expected props
    expect(TableVirtuoso).toHaveBeenCalledWith(
      expect.objectContaining({
        totalCount: mockRows.length,
        style: { minHeight: 120 },
        initialTopMostItemIndex: undefined,
        fixedItemHeight: 40,
        increaseViewportBy: 50,
        useWindowScroll: true,
      }),
      expect.anything()
    );
  });

  it("should set up headerContent correctly", () => {
    render(
      <TestWrapper>
        <SyncCatalogVirtuosoTable {...defaultProps} />
      </TestWrapper>
    );

    // headerContent is called by our mock implementation
    expect(mockGetHeaderGroups).toHaveBeenCalled();
  });

  it("should call setStickyRowIndex when atTopStateChange is triggered and stickyRowIndex is not 0", () => {
    const setStickyRowIndexMock = jest.fn();

    render(
      <TestWrapper>
        <SyncCatalogVirtuosoTable
          {...defaultProps}
          setStickyRowIndex={setStickyRowIndexMock}
          stickyRowIndex={1} // Setting to non-zero value to satisfy the condition
        />
      </TestWrapper>
    );

    // Extract the atTopStateChange callback from the call to TableVirtuoso
    const mockCall = (TableVirtuoso as jest.Mock).mock.calls[0][0];
    const atTopStateChange = mockCall.atTopStateChange;

    // Call the function manually with true, which should trigger setStickyRowIndex
    atTopStateChange(true);

    expect(setStickyRowIndexMock).toHaveBeenCalledWith(0);
  });

  it("should not call setStickyRowIndex when atTopStateChange is false", () => {
    const setStickyRowIndexMock = jest.fn();
    render(
      <TestWrapper>
        <SyncCatalogVirtuosoTable {...defaultProps} setStickyRowIndex={setStickyRowIndexMock} stickyRowIndex={1} />
      </TestWrapper>
    );

    const mockProps = (TableVirtuoso as jest.Mock).mock.calls[0][0];
    mockProps.atTopStateChange(false);

    expect(setStickyRowIndexMock).not.toHaveBeenCalled();
  });

  it("should not call setStickyRowIndex when atTopStateChange is true but stickyRowIndex is already 0", () => {
    const setStickyRowIndexMock = jest.fn();
    render(
      <TestWrapper>
        <SyncCatalogVirtuosoTable
          {...defaultProps}
          setStickyRowIndex={setStickyRowIndexMock}
          stickyRowIndex={0} // Setting to 0 which should prevent the call
        />
      </TestWrapper>
    );

    const mockProps = (TableVirtuoso as jest.Mock).mock.calls[0][0];
    mockProps.atTopStateChange(true);

    expect(setStickyRowIndexMock).not.toHaveBeenCalled();
  });

  it("should handle empty rows", () => {
    render(
      <TestWrapper>
        <SyncCatalogVirtuosoTable {...defaultProps} rows={[]} />
      </TestWrapper>
    );

    expect(screen.getByTestId("mocked-table-virtuoso")).toBeInTheDocument();

    const mockProps = (TableVirtuoso as jest.Mock).mock.calls[0][0];
    expect(mockProps.totalCount).toBe(0);

    // Verify the EmptyPlaceholder is showing the "no streams" message
    expect(screen.getByText("No streams")).toBeInTheDocument();
  });

  it("should show 'no matching streams' message when globalFilter has a value", () => {
    // Setup mock to return a globalFilter with a value
    mockGetState.mockReturnValueOnce({
      globalFilter: "test",
    });

    render(
      <TestWrapper>
        <SyncCatalogVirtuosoTable {...defaultProps} rows={[]} />
      </TestWrapper>
    );

    // Verify the EmptyPlaceholder is showing the "no matching streams" message
    expect(screen.getByText("No matching streams")).toBeInTheDocument();
  });

  it("should show 'no streams' message when globalFilter is empty", () => {
    // Setup mock to return a globalFilter with empty value
    mockGetState.mockReturnValueOnce({
      globalFilter: "",
    });

    render(
      <TestWrapper>
        <SyncCatalogVirtuosoTable {...defaultProps} rows={[]} />
      </TestWrapper>
    );

    // Verify the EmptyPlaceholder is showing the "no streams" message
    expect(screen.getByText("No streams")).toBeInTheDocument();
  });

  it("should pass globalFilter to context", () => {
    // Setup mock to return a globalFilter
    mockGetState.mockReturnValueOnce({
      globalFilter: "test",
    });

    render(
      <TestWrapper>
        <SyncCatalogVirtuosoTable {...defaultProps} />
      </TestWrapper>
    );

    // We can verify that TableVirtuoso received the context with the filteringValue
    const mockCall = (TableVirtuoso as jest.Mock).mock.calls[0][0];
    expect(mockCall.context).toMatchObject({
      filteringValue: "test",
    });
  });
});
