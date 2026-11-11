import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { FailureOrigin, FailureReason, FailureType, JobConfigType } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";
import { downloadFile } from "core/utils/file";

import { TestFailurePanel } from "./TestFailurePanel";

jest.mock("core/utils/file", () => ({ downloadFile: jest.fn() }));
jest.mock("core/utils/datadog", () => ({ trackError: jest.fn() }));

const FAILURE_REASON: FailureReason = {
  externalMessage: "The host db.example.com is not reachable.",
  internalMessage: "java.net.UnknownHostException: db.example.com",
  failureOrigin: FailureOrigin.source,
  failureType: FailureType.config_error,
  stacktrace: "java.net.UnknownHostException: db.example.com\n\tat Check.run(Check.java:12)",
  timestamp: 0,
};

const renderPanel = async (props: Partial<React.ComponentProps<typeof TestFailurePanel>> = {}) => {
  const onClose = jest.fn();
  await render(
    <TestFailurePanel
      failedStep="test"
      fallbackMessage="Could not connect"
      jobId="command-id"
      jobConfigType={JobConfigType.check_connection_source}
      jobLogs={{ logLines: ["Connecting to db.example.com", "Connection refused"] }}
      failureReason={FAILURE_REASON}
      onClose={onClose}
      {...props}
    />
  );
  return { onClose };
};

describe("TestFailurePanel", () => {
  beforeEach(() => {
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText: jest.fn().mockResolvedValue(undefined) },
      writable: true,
      configurable: true,
    });
  });

  it("shows the external message of the failed check under its heading", async () => {
    await renderPanel();

    expect(screen.getByRole("heading", { name: "Testing failed." })).toBeInTheDocument();
    expect(screen.getByText(FAILURE_REASON.externalMessage!)).toBeInTheDocument();
    expect(screen.queryByText("Could not connect")).not.toBeInTheDocument();
  });

  it("shows the fallback message when the check returned no external message", async () => {
    await renderPanel({ failureReason: undefined });

    expect(screen.getByText("Could not connect")).toBeInTheDocument();
  });

  it("calls onClose from the close button", async () => {
    const { onClose } = await renderPanel();

    await userEvent.click(screen.getByRole("button", { name: "Close" }));

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("copies every failure detail as plain text and confirms the copy", async () => {
    await renderPanel();

    await userEvent.click(screen.getByRole("button", { name: "Copy error" }));

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      [
        "The host db.example.com is not reachable.",
        "Internal message: java.net.UnknownHostException: db.example.com",
        "Failure origin: source",
        "Failure type: config_error",
        "Stacktrace: java.net.UnknownHostException: db.example.com\n\tat Check.run(Check.java:12)",
      ].join("\n")
    );
    expect(await screen.findByRole("button", { name: "Copied" })).toBeInTheDocument();
  });

  it("copies only the details that are set", async () => {
    await renderPanel({ failureReason: { failureOrigin: FailureOrigin.destination, timestamp: 0 } });

    await userEvent.click(screen.getByRole("button", { name: "Copy error" }));

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith("Could not connect\nFailure origin: destination");
  });

  // `navigator.clipboard` is undefined outside a secure context, for example on an instance served over plain HTTP
  it("shows an error notification instead of the confirmation when the clipboard API is unavailable", async () => {
    Object.defineProperty(navigator, "clipboard", { value: undefined, writable: true, configurable: true });
    await renderPanel();

    await userEvent.click(screen.getByRole("button", { name: "Copy error" }));

    expect(await screen.findByText("Could not copy the error to the clipboard")).toBeInTheDocument();
    expect(trackError).toHaveBeenCalledWith(expect.any(TypeError), { id: "connectorForm.testFailure.copyError" });
    expect(screen.getByRole("button", { name: "Copy error" })).toBeInTheDocument();
  });

  it("opens the logs modal from View logs", async () => {
    await renderPanel();

    await userEvent.click(screen.getByRole("button", { name: "View logs" }));

    expect(await screen.findByText("Logs: check_connection_source job")).toBeInTheDocument();
  });

  it("downloads the log lines from Download logs", async () => {
    await renderPanel();

    await userEvent.click(screen.getByRole("button", { name: "Download logs" }));

    expect(downloadFile).toHaveBeenCalledWith(expect.any(Blob), "check_connection_source-failure-command-id.txt");
  });

  it("offers no log actions when the check returned no logs", async () => {
    await renderPanel({ jobLogs: undefined });

    expect(screen.getByRole("button", { name: "Copy error" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "View logs" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Download logs" })).not.toBeInTheDocument();
  });
});
