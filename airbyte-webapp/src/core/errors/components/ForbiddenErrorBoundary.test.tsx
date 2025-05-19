import { screen } from "@testing-library/react";
import React from "react";

import { render } from "test-utils";

import { HttpError } from "core/api";

import { ForbiddenErrorBoundary } from "./ForbiddenErrorBoundary";

// Mock the authentication service
jest.mock("core/services/auth", () => {
  return {
    useAuthService: jest.fn().mockReturnValue({
      logout: jest.fn(),
      login: jest.fn(),
      isLoggedIn: jest.fn().mockReturnValue(true),
    }),
    AuthenticationServiceProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  };
});

// Mock the AirbyteTheme
jest.mock("hooks/theme/useAirbyteTheme", () => {
  const themeContextValue = {
    theme: "airbyteThemeLight",
    colorValues: {},
    setTheme: jest.fn(),
  };

  return {
    AirbyteThemeProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
    useAirbyteTheme: jest.fn().mockReturnValue(themeContextValue),
    AirbyteThemeContext: {
      Provider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
    },
  };
});

// Mock useNavigate for navigation tests
const mockNavigate = jest.fn();
jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useNavigate: () => mockNavigate,
}));

// Create test errors
const mockWorkspace403Error = new HttpError({ url: "/api/v1/workspaces/get", method: "GET" }, 403, "Forbidden");
const mockNonWorkspace403Error = new HttpError({ url: "/api/v1/other", method: "GET" }, 403, "Forbidden");
const mock500Error = new HttpError({ url: "test", method: "GET" }, 500, "Server Error");

const ChildThatThrowsWorkspace403Error = () => {
  throw mockWorkspace403Error;
};

const ChildThatThrowsNonWorkspace403Error = () => {
  throw mockNonWorkspace403Error;
};

const ChildThatThrows500Error = () => {
  throw mock500Error;
};

// Simple wrapper component
const ForbiddenErrorBoundaryWithContext = ({ children }: { children: React.ReactNode }) => (
  <ForbiddenErrorBoundary>{children}</ForbiddenErrorBoundary>
);

describe(`${ForbiddenErrorBoundary.name}`, () => {
  let originalConsoleError: typeof console.error;
  let originalConsoleLog: typeof console.log;

  beforeAll(() => {
    originalConsoleError = console.error;
    originalConsoleLog = console.log;
    console.error = jest.fn();
    console.log = jest.fn();
  });

  afterAll(() => {
    console.error = originalConsoleError;
    console.log = originalConsoleLog;
  });

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should render children when no error is thrown", async () => {
    const result = await render(
      <ForbiddenErrorBoundaryWithContext>
        <p data-testid="test-content">test content</p>
      </ForbiddenErrorBoundaryWithContext>
    );

    expect(result.getByTestId("test-content")).toBeInTheDocument();
  });

  it("should render error view when a workspace-specific 403 error is thrown", async () => {
    const result = await render(
      <ForbiddenErrorBoundaryWithContext>
        <ChildThatThrowsWorkspace403Error />
      </ForbiddenErrorBoundaryWithContext>
    );

    expect(result.getByTestId("forbidden-error-boundary-button")).toBeInTheDocument();
  });

  it("should not catch non-workspace 403 errors", async () => {
    const errorHandler = jest.fn();
    window.addEventListener("error", (event) => {
      event.preventDefault();
      errorHandler(event.error);
      return true;
    });

    try {
      await render(
        <ForbiddenErrorBoundaryWithContext>
          <ChildThatThrowsNonWorkspace403Error />
        </ForbiddenErrorBoundaryWithContext>
      );
    } catch (error) {
      // Expected to throw
    }

    expect(errorHandler).toHaveBeenCalled();
    expect(screen.queryByTestId("forbidden-error-boundary-button")).not.toBeInTheDocument();

    window.removeEventListener("error", errorHandler);
  });

  it("should not catch non-403 errors", async () => {
    const errorHandler = jest.fn();
    window.addEventListener("error", (event) => {
      event.preventDefault();
      errorHandler(event.error);
      return true;
    });

    try {
      await render(
        <ForbiddenErrorBoundaryWithContext>
          <ChildThatThrows500Error />
        </ForbiddenErrorBoundaryWithContext>
      );
    } catch (error) {
      // Expected to throw
    }

    expect(errorHandler).toHaveBeenCalled();
    expect(screen.queryByTestId("forbidden-error-boundary-button")).not.toBeInTheDocument();

    window.removeEventListener("error", errorHandler);
  });
});
