import { render, screen, fireEvent } from "@testing-library/react";
import React from "react";

import { TestWrapper } from "test-utils/testutils";

import { ConnectionFormMode, useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { SearchAndFilterControls } from "./SearchAndFilterControls";
import { FilterTabId } from "./StreamsFilterTabs";

// Mock the dependencies that cause issues
jest.mock("hooks/services/ConnectionForm/ConnectionFormService", () => ({
  useConnectionFormService: jest.fn(),
}));
jest.mock("core/services/ui/FormModeContext", () => ({
  useFormMode: () => ({
    mode: "edit",
  }),
}));
// Mock the components that use react-hook-form
jest.mock("./RefreshSchemaControl", () => ({
  RefreshSchemaControl: () => <button data-testid="refresh-schema-button">Refresh Schema</button>,
}));

jest.mock("./FormControls", () => ({
  FormControls: ({ children }: React.PropsWithChildren) => <div data-testid="form-controls">{children}</div>,
}));

interface ExpandCollapseProps {
  isAllRowsExpanded: boolean;
  toggleAllRowsExpanded: (expanded: boolean) => void;
}

jest.mock("./ExpandCollapseAllControl", () => ({
  ExpandCollapseAllControl: ({ isAllRowsExpanded, toggleAllRowsExpanded }: ExpandCollapseProps) => (
    <button data-testid="expand-all-button" onClick={() => toggleAllRowsExpanded(!isAllRowsExpanded)}>
      {isAllRowsExpanded ? "Collapse All" : "Expand All"}
    </button>
  ),
}));

interface StreamsFilterTabsProps {
  onTabSelect: (tabId: FilterTabId) => void;
}

// Also mock the StreamsFilterTabs component to avoid potential issues
jest.mock("./StreamsFilterTabs", () => ({
  FilterTabId: { all: "all", enabledStreams: "enabledStreams", disabledStreams: "disabledStreams" },
  StreamsFilterTabs: ({ onTabSelect }: StreamsFilterTabsProps) => (
    <div role="tablist">
      <button role="tab" onClick={() => onTabSelect("all")}>
        All
      </button>
      <button role="tab" onClick={() => onTabSelect("enabledStreams")}>
        Enabled
      </button>
      <button role="tab" onClick={() => onTabSelect("disabledStreams")}>
        Disabled
      </button>
    </div>
  ),
}));

describe("SearchAndFilterControls", () => {
  const mockUseConnectionFormService = useConnectionFormService as jest.Mock;

  const defaultProps = {
    filtering: "",
    setFiltering: jest.fn(),
    filteringDepth: 100,
    setFilteringDepth: jest.fn(),
    isAllStreamRowsExpanded: false,
    toggleAllStreamRowsExpanded: jest.fn(),
    columnFilters: [],
    onTabSelect: jest.fn(),
  };

  beforeEach(() => {
    jest.clearAllMocks();
    // Default mock implementation for the hook
    mockUseConnectionFormService.mockImplementation(() => ({
      mode: "create",
      connection: {},
    }));
  });

  const renderComponent = (props = {}, mode: ConnectionFormMode = "create") => {
    mockUseConnectionFormService.mockImplementation(() => ({
      mode,
      connection: {},
    }));

    return render(
      <TestWrapper>
        <SearchAndFilterControls {...defaultProps} {...props} />
      </TestWrapper>
    );
  };

  it("should render search input", () => {
    renderComponent();

    expect(screen.getByTestId("sync-catalog-search")).toBeInTheDocument();
  });

  it("should call setFiltering when search input changes", () => {
    renderComponent();

    const searchInput = screen.getByTestId("sync-catalog-search");
    fireEvent.change(searchInput, { target: { value: "test" } });

    expect(defaultProps.setFiltering).toHaveBeenCalled();
  });

  it("should render RefreshSchemaControl and ExpandCollapseAllControl in create mode", () => {
    renderComponent({}, "create");

    const refreshControl = screen.getByTestId("refresh-schema-button");
    expect(refreshControl).toBeInTheDocument();

    const expandCollapseControl = screen.getByTestId("expand-all-button");
    expect(expandCollapseControl).toBeInTheDocument();
  });

  it("should render RefreshSchemaControl inside FormControls in edit mode", () => {
    renderComponent({}, "edit");

    const refreshControl = screen.getByTestId("refresh-schema-button");
    expect(refreshControl).toBeInTheDocument();

    const formControls = screen.getByTestId("form-controls");
    expect(formControls).toBeInTheDocument();

    const expandCollapseControl = screen.getByTestId("expand-all-button");
    expect(expandCollapseControl).toBeInTheDocument();
  });

  it("should render StreamsFilterTabs", () => {
    renderComponent();

    expect(screen.getByRole("tablist")).toBeInTheDocument();
  });

  it("should call onTabSelect when tab is clicked", () => {
    renderComponent();

    const allTab = screen.getByRole("tab", { name: /all/i });
    fireEvent.click(allTab);

    expect(defaultProps.onTabSelect).toHaveBeenCalled();
  });
});
