import React from "react";
import { NavigateFunction, useNavigate } from "react-router-dom";
import { useLocation } from "react-use";
import { LocationSensorState } from "react-use/lib/useLocation";

import { HttpError } from "core/api";
import { trackError } from "core/utils/datadog";
import { TrackErrorFn } from "hooks/services/AppMonitoringService";

import { ErrorDetails } from "./ErrorDetails";

interface ErrorBoundaryState {
  error?: Error;
  message?: string;
}

interface ErrorBoundaryHookProps {
  location: LocationSensorState;
  navigate: NavigateFunction;
  trackError: TrackErrorFn;
}

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
  state: ErrorBoundaryState = {};

  static getDerivedStateFromError(error: unknown): ErrorBoundaryState {
    return { error: error instanceof Error ? error : new WrappedError(error) };
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
      this.props.trackError(error);
    }
  }

  override render(): React.ReactNode {
    const { error } = this.state;
    return error ? <ErrorDetails error={error} /> : this.props.children;
  }
}

export const DefaultErrorBoundary: React.FC<React.PropsWithChildren> = ({ children }) => {
  const location = useLocation();
  const navigate = useNavigate();

  return (
    <ErrorBoundaryComponent location={location} navigate={navigate} trackError={trackError}>
      {children}
    </ErrorBoundaryComponent>
  );
};
