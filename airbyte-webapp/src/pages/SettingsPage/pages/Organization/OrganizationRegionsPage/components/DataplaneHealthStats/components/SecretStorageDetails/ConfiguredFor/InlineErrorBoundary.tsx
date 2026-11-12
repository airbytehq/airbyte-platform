import React from "react";

interface InlineErrorBoundaryProps {
  fallback: React.ReactNode;
}

interface InlineErrorBoundaryState {
  hasError: boolean;
}

// Shows the fallback in place of children that fail to render, so one failing part does not take the page with it.
export class InlineErrorBoundary extends React.Component<
  React.PropsWithChildren<InlineErrorBoundaryProps>,
  InlineErrorBoundaryState
> {
  state: InlineErrorBoundaryState = { hasError: false };

  static getDerivedStateFromError(): InlineErrorBoundaryState {
    return { hasError: true };
  }

  override render(): React.ReactNode {
    return this.state.hasError ? this.props.fallback : this.props.children;
  }
}
