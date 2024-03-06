import React from "react";

import { CommonRequestError } from "core/api";
import { TrackErrorFn } from "hooks/services/AppMonitoringService";

interface BoundaryState {
  hasError: boolean;
  message?: React.ReactNode | null;
}

const initialState: BoundaryState = {
  hasError: false,
  message: null,
};

interface InsufficientPermissionsErrorBoundaryProps {
  errorComponent: React.ReactElement;
  trackError: TrackErrorFn;
}

export class InsufficientPermissionsErrorBoundary extends React.Component<
  React.PropsWithChildren<InsufficientPermissionsErrorBoundaryProps>,
  BoundaryState
> {
  static getDerivedStateFromError(error: CommonRequestError): BoundaryState {
    if (error.message.startsWith("Insufficient permissions")) {
      return { hasError: true, message: error.message };
    }
    throw error;
  }

  componentDidCatch(error: Error): void {
    this.props.trackError(error, { errorBoundary: this.constructor.name });
  }

  state = initialState;

  reset = (): void => {
    this.setState(initialState);
  };

  render(): React.ReactNode {
    return this.state.hasError
      ? React.cloneElement(this.props.errorComponent, {
          message: this.state.message,
          onReset: this.reset,
        })
      : this.props.children;
  }
}
