import React from "react";
import { NavigateFunction, useNavigate } from "react-router-dom";
import { useLocation } from "react-use";
import { LocationSensorState } from "react-use/lib/useLocation";

import { HttpError } from "core/api";
import { trackError } from "core/utils/datadog";

import { ErrorDetails } from "./ErrorDetails";
import { WaitForRetry } from "./WaitForRetry";

interface ErrorBoundaryState {
  error?: Error;
  waitForRetry: boolean;
}

interface ErrorBoundaryHookProps {
  location: LocationSensorState;
  navigate: NavigateFunction;
}

const SESSION_STORAGE_KEY = "airbyte-last-error-retry";
/**
 * Minimum time in ms since the last automatic reload before we'd reload again in case of an async import error.
 */
const RETRY_TIMEOUT = 10000;

/**
 * Checks whether an error is a failure to fetch an async imported module.
 * In Chrome the error message will be: "Failed to fetch dynamically imported module: <URL>"
 * In Firefox: "error loading dynamically imported module: <URL>"
 * In Safari: "Importing a module script failed"
 */
const isDyanmicImportError = (error: unknown): boolean => {
  return (
    error instanceof Error &&
    (error.message.includes("Unable to preload CSS") ||
      (error.name === "TypeError" &&
        (error.message.includes("dynamically imported module") ||
          error.message.includes("Importing a module script failed"))))
  );
};

class WrappedError extends Error {
  constructor(public readonly cause: unknown) {
    const message = typeof cause === "string" ? cause : "Non Error object thrown";
    super(message);
  }
}

class ErrorBoundaryComponent extends React.Component<
  React.PropsWithChildren<ErrorBoundaryHookProps>,
  ErrorBoundaryState
> {
  state: ErrorBoundaryState = { waitForRetry: false };

  static getDerivedStateFromError(error: unknown): ErrorBoundaryState {
    if (
      isDyanmicImportError(error) &&
      Number(sessionStorage.getItem(SESSION_STORAGE_KEY) ?? "0") < Date.now() - RETRY_TIMEOUT
    ) {
      // If the error is due to a async import failed and the last retry was more than $RETRY_TIMEOUT ms ago
      // do use the retry component (which will reload the page) instead of showing the error.
      return { error: undefined, waitForRetry: true };
    }
    return { error: error instanceof Error ? error : new WrappedError(error), waitForRetry: false };
  }

  override componentDidUpdate(prevProps: ErrorBoundaryHookProps) {
    // Clear out the error in case the user navigates to another part of the app
    if (this.props.location !== prevProps.location) {
      this.setState({ error: undefined });
    }
  }

  override componentDidCatch(error: Error) {
    if (!(error instanceof HttpError)) {
      // Only track non HttpErrors here, since we already track HttpErrors in the apiCall
      // method, so that we catch them also in case they aren't handled by an error boundary,
      // but e.g. just will result in a toast notification
      trackError(error);
    }
  }

  private reloadPage() {
    sessionStorage.setItem(SESSION_STORAGE_KEY, Date.now().toString());
    window.location.reload();
  }

  override render(): React.ReactNode {
    const { error, waitForRetry } = this.state;

    if (waitForRetry) {
      return <WaitForRetry retry={this.reloadPage.bind(this)} />;
    }

    if (error) {
      return <ErrorDetails error={error} />;
    }

    return this.props.children;
  }
}

export const DefaultErrorBoundary: React.FC<React.PropsWithChildren> = ({ children }) => {
  const location = useLocation();
  const navigate = useNavigate();

  return (
    <ErrorBoundaryComponent location={location} navigate={navigate}>
      {children}
    </ErrorBoundaryComponent>
  );
};
