import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import "@testing-library/jest-dom";
import { DownloadLogsButton } from "../src/area/connection/components/JobLogsModal/DownloadLogsButton";

// Mocks de dependencias
jest.mock("react-intl", () => ({
  FormattedMessage: ({ id }: any) => <span>{id}</span>,
  useIntl: () => ({
    formatMessage: ({ id }: any) => id,
  }),
}));

jest.mock("components/ui/Button", () => ({
  Button: ({ onClick, "aria-label": ariaLabel }: any) => (
    <button data-testid="download-button" onClick={onClick} aria-label={ariaLabel}>
      Download
    </button>
  ),
}));

jest.mock("components/ui/Tooltip", () => ({
  Tooltip: ({ control, children }: any) => (
    <div data-testid="tooltip">
      <div data-testid="tooltip-control">{control}</div>
      <div data-testid="tooltip-text">{children}</div>
    </div>
  ),
}));


describe("DownloadLogsButton", () => {
  const mockDownloadLogs = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renderiza el botón de descarga y el texto del tooltip", () => {
    render(<DownloadLogsButton downloadLogs={mockDownloadLogs} />);

    expect(screen.getByTestId("tooltip")).toBeInTheDocument();
    expect(screen.getByTestId("tooltip-text")).toHaveTextContent("jobHistory.logs.downloadLogs");
    expect(screen.getByTestId("download-button")).toHaveAttribute("aria-label", "jobHistory.logs.downloadLogs");
  });

  it("llama a downloadLogs cuando se hace clic en el botón", () => {
    render(<DownloadLogsButton downloadLogs={mockDownloadLogs} />);
    fireEvent.click(screen.getByTestId("download-button"));
    expect(mockDownloadLogs).toHaveBeenCalledTimes(1);
  });
});
