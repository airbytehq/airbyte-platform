import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import MainView from "../src/views/Layout/MainView/MainView";

declare const require: any;

// Mocks de dependencias
jest.mock("react-router-dom", () => ({
  Outlet: () => <div data-testid="outlet">Outlet rendered</div>,
}));

jest.mock("components", () => ({
  LoadingPage: () => <div data-testid="loading-page">Loading...</div>,
}));

jest.mock("components/LicenseBanner/LicenseBanner", () => ({
  LicenseBanner: () => <div data-testid="license-banner">LicenseBanner</div>,
}));

jest.mock("components/ui/Flex", () => ({
  FlexContainer: ({ children, className }: any) => (
    <div className={className} data-testid="flex-container">
      {children}
    </div>
  ),
}));

jest.mock("core/errors", () => ({
  DefaultErrorBoundary: ({ children }: any) => <div data-testid="default-boundary">{children}</div>,
  ForbiddenErrorBoundary: ({ children }: any) => <div data-testid="forbidden-boundary">{children}</div>,
}));

jest.mock("hooks/services/useConnector", () => ({
  useGetConnectorsOutOfDate: jest.fn(),
}));

jest.mock("../src/views/Layout/SideBar/SideBar", () => ({
  SideBar: ({ bottomSlot, settingHighlight }: any) => (
    <div data-testid="sidebar">
      Sidebar - highlight:{String(settingHighlight)}
      {bottomSlot}
    </div>
  ),
}));

jest.mock("../src/views/Layout/SideBar/components/HelpDropdown", () => ({
  HelpDropdown: () => <div data-testid="help-dropdown">HelpDropdown</div>,
}));

// 🧪 TESTS
describe("MainView", () => {
  const mockUseGetConnectorsOutOfDate = require("hooks/services/useConnector").useGetConnectorsOutOfDate;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renderiza todos los componentes principales", () => {
    mockUseGetConnectorsOutOfDate.mockReturnValue({ hasNewVersions: false });

    render(<MainView />);

    expect(screen.getByTestId("license-banner")).toBeInTheDocument();
    expect(screen.getByTestId("sidebar")).toBeInTheDocument();
    expect(screen.getByTestId("help-dropdown")).toBeInTheDocument();
    expect(screen.getByTestId("outlet")).toBeInTheDocument();
    expect(screen.getByTestId("forbidden-boundary")).toBeInTheDocument();
    expect(screen.getByTestId("default-boundary")).toBeInTheDocument();
  });

  it("pasa correctamente el valor de hasNewVersions al SideBar", () => {
    mockUseGetConnectorsOutOfDate.mockReturnValue({ hasNewVersions: true });

    render(<MainView />);

    const sidebar = screen.getByTestId("sidebar");
    expect(sidebar).toHaveTextContent("highlight:true");
  });
});
