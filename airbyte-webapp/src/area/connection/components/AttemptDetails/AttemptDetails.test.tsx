import { screen } from "@testing-library/react";

import { render } from "test-utils";
import { mockAttempt } from "test-utils/mock-data/mockAttempt";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useAttemptCombinedStatsForJob, useCurrentConnection, useRejectedRecordsForJob } from "core/api";
import { AttemptStatus, FailureOrigin, FailureType } from "core/api/types/AirbyteClient";

import { AttemptDetails } from "./AttemptDetails";

// Mock the hooks
jest.mock("core/api", () => ({
  useAttemptCombinedStatsForJob: jest.fn(),
  useRejectedRecordsForJob: jest.fn(),
  useCurrentConnection: jest.fn(),
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceLink: jest.fn(),
  useCurrentWorkspaceId: jest.fn(),
}));

jest.mock("core/utils/time", () => ({
  useFormatLengthOfTime: jest.fn(() => "5 minutes"),
}));

describe("AttemptDetails", () => {
  const defaultProps = {
    attempt: {
      ...mockAttempt,
      status: AttemptStatus.succeeded,
      totalStats: {
        recordsEmitted: 100,
        recordsCommitted: 95,
        bytesEmitted: 1024,
      },
    },
    jobId: 123,
  };

  beforeEach(() => {
    (useAttemptCombinedStatsForJob as jest.Mock).mockReturnValue({
      data: {
        recordsEmitted: 100,
        recordsCommitted: 95,
        bytesEmitted: 1024,
      },
    });
    (useCurrentConnection as jest.Mock).mockReturnValue({
      destination: {
        destinationId: "test-destination",
        name: "test destination",
      },
      connection: {
        connectionId: "test-connection",
        name: "test connection",
      },
    });
    (useCurrentWorkspaceLink as jest.Mock).mockReturnValue(jest.fn((path) => path));
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it("renders attempt details without rejected records", async () => {
    (useRejectedRecordsForJob as jest.Mock).mockReturnValue({
      recordsRejected: null,
      rejectedRecordsMeta: null,
    });
    await render(<AttemptDetails {...defaultProps} />);

    expect(screen.queryByText("rejected records")).not.toBeInTheDocument();
    expect(screen.getByText("100 records extracted")).toBeInTheDocument();
    expect(screen.getByText("95 records loaded")).toBeInTheDocument();
    expect(screen.getByText("1 kB")).toBeInTheDocument();
    expect(screen.getByText("Job id: 123")).toBeInTheDocument();
    expect(screen.getByText("5 minutes")).toBeInTheDocument();
  });

  it("shows rejected records UI when useRejectedRecordsForJob returns rejected records info", async () => {
    (useRejectedRecordsForJob as jest.Mock).mockReturnValue({
      recordsRejected: 5,
      rejectedRecordsMeta: {
        cloudConsoleUrl: "https://console.example.com/rejected-records",
      },
    });

    await render(<AttemptDetails {...defaultProps} />);

    expect(screen.getByText("5 records rejected")).toBeInTheDocument();
    const rejectedLink = screen.getByRole("link");
    expect(rejectedLink).toHaveAttribute("href", "https://console.example.com/rejected-records");
  });

  it("does not show rejected records UI when no rejected records", async () => {
    (useRejectedRecordsForJob as jest.Mock).mockReturnValue({
      recordsRejected: 0,
      rejectedRecordsMeta: null,
    });

    await render(<AttemptDetails {...defaultProps} />);

    expect(screen.queryByText(/rejected/)).not.toBeInTheDocument();
  });

  it("does not render when attempt status is neither succeeded nor failed", async () => {
    const props = {
      ...defaultProps,
      attempt: {
        ...defaultProps.attempt,
        status: AttemptStatus.running,
      },
    };

    await render(<AttemptDetails {...props} />);

    expect(screen.queryByText("Job id: 123")).not.toBeInTheDocument();
    expect(screen.queryByText("100 records extracted")).not.toBeInTheDocument();
  });

  it("renders failure message for failed attempts", async () => {
    const props = {
      ...defaultProps,
      attempt: {
        ...defaultProps.attempt,
        status: AttemptStatus.failed,
        failureSummary: {
          failures: [
            {
              failureOrigin: FailureOrigin.source,
              externalMessage: "Connection timeout",
              failureType: FailureType.system_error,
              timestamp: 1696166400,
            },
          ],
        },
      },
    };

    await render(<AttemptDetails {...props} />);

    // The error message is formatted as "Failure Origin: source, Message: Connection timeout"
    expect(screen.getByText(/Failure Origin: source, Message: Connection timeout/)).toBeInTheDocument();
  });
});
