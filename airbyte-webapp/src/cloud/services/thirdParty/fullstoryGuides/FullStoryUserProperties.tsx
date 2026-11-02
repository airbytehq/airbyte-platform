import React, { Suspense } from "react";

import { useFullStoryUserProperties } from "./useFullStoryUserProperties";

class FullStoryUserPropertiesErrorBoundary extends React.Component<React.PropsWithChildren, { hasError: boolean }> {
  state = { hasError: false };

  static getDerivedStateFromError() {
    return { hasError: true };
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
