import React, { Suspense } from "react";

import { trackError } from "core/utils/datadog";

import { useFullStoryUserProperties } from "./useFullStoryUserProperties";

class FullStoryUserPropertiesErrorBoundary extends React.Component<React.PropsWithChildren, { hasError: boolean }> {
  state = { hasError: false };

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  override componentDidCatch(error: Error) {
    trackError(error, { component: "FullStoryUserProperties" });
  }

  override render(): React.ReactNode {
    return this.state.hasError ? null : this.props.children;
  }
}

const FullStoryUserPropertiesSync: React.FC = () => {
  useFullStoryUserProperties();
  return null;
};

export const FullStoryUserProperties: React.FC = () => (
  <FullStoryUserPropertiesErrorBoundary>
    <Suspense fallback={null}>
      <FullStoryUserPropertiesSync />
    </Suspense>
  </FullStoryUserPropertiesErrorBoundary>
);
