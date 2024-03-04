import { screen } from "@testing-library/react";

import { mocked, render } from "test-utils";

import { trackError } from "core/utils/datadog";
import { AppMonitoringServiceProvider } from "hooks/services/AppMonitoringService";

import { ApiErrorBoundary } from "./ApiErrorBoundary";

const mockError = new Error("oh no!");

jest.mock("core/utils/datadog", () => ({
  trackError: jest.fn(),
}));

const ChildThatThrowsError = () => {
  throw mockError;
};

describe(`${ApiErrorBoundary.name}`, () => {
  let originalConsoleDebug: typeof console.debug;
  let originalConsoleError: typeof console.error;

  beforeAll(() => {
    originalConsoleDebug = console.debug;
    originalConsoleError = console.error;
    console.debug = jest.fn();
    console.error = jest.fn();
  });

  afterAll(() => {
    console.error = originalConsoleError;
    console.debug = originalConsoleDebug;
  });

  it("should render children when no error is thrown", async () => {
    await render(
      <AppMonitoringServiceProvider>
        <ApiErrorBoundary>
          <p>test</p>
        </ApiErrorBoundary>
      </AppMonitoringServiceProvider>
    );

    expect(screen.getByText("test")).toBeInTheDocument();
  });

  it("should render error view when an error is thrown", async () => {
    await render(
      <AppMonitoringServiceProvider>
        <ApiErrorBoundary>
          <ChildThatThrowsError />
        </ApiErrorBoundary>
      </AppMonitoringServiceProvider>
    );

    expect(screen.getByTestId("errorView")).toBeInTheDocument();
  });

  it("should log the error when it throws", async () => {
    const mockTrackError = mocked(trackError);
    mockTrackError.mockClear();

    await render(
      <ApiErrorBoundary>
        <ChildThatThrowsError />
      </ApiErrorBoundary>
    );

    expect(mockTrackError).toHaveBeenCalledTimes(1);
    expect(mockTrackError).toHaveBeenCalledWith(mockError, expect.anything());
  });
});
