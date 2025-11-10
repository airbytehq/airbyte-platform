import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import "@testing-library/jest-dom";
import { NoDataMessage } from "../src/area/connection/components/HistoricalOverview/NoDataMessage";

declare const require: any;

// Mocks de dependencias
const mockSyncConnection = jest.fn();

jest.mock("react-intl", () => ({
  FormattedMessage: ({ id }: any) => <span>{id}</span>,
}));

jest.mock("components/EmptyState", () => ({
  EmptyState: ({ icon, text, button }: any) => (
    <div data-testid="empty-state">
      <div data-testid="icon">{icon}</div>
      <div data-testid="text">{text}</div>
      <div data-testid="button">{button}</div>
    </div>
  ),
}));

jest.mock("components/ui/Button", () => ({
  Button: ({ children, disabled, onClick }: any) => (
    <button disabled={disabled} onClick={onClick}>
      {children}
    </button>
  ),
}));

jest.mock("components/connection/ConnectionSync/ConnectionSyncContext", () => ({
  useConnectionSyncContext: () => ({
    syncConnection: mockSyncConnection,
    isSyncConnectionAvailable: true,
  }),
}));

jest.mock("core/api", () => ({
  useCurrentConnection: () => ({
    status: "active",
  }),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: { RunAndCancelConnectionSyncAndRefresh: "RunAndCancelConnectionSyncAndRefresh" },
  useGeneratedIntent: () => true,
}));


describe("NoDataMessage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renderiza el mensaje y el botón correctamente", () => {
    render(<NoDataMessage />);
    expect(screen.getByTestId("empty-state")).toBeInTheDocument();
    expect(screen.getByText("connection.overview.graph.noData")).toBeInTheDocument();
    expect(screen.getByText("connection.overview.graph.noData.button")).toBeInTheDocument();
  });

  it("llama a syncConnection al hacer clic en el botón si está habilitado", () => {
    render(<NoDataMessage />);
    const button = screen.getByRole("button");
    fireEvent.click(button);
    expect(mockSyncConnection).toHaveBeenCalled();
  });

});
