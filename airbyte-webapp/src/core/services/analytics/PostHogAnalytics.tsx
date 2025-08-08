import { PostHog } from "posthog-js";
import { PostHogProvider } from "posthog-js/react";
import React from "react";

import { useWebappConfig } from "core/config";

declare global {
  interface Window {
    posthog: PostHog;
  }
}
export const PostHogAnalytics: React.FC<React.PropsWithChildren> = ({ children }) => {
  const config = useWebappConfig();
  if (!config.posthogApiKey) {
    return <>{children}</>;
  }

  return (
    <PostHogProvider
      apiKey={config.posthogApiKey}
      options={{
        api_host: config.posthogHost || "",
        capture_pageview: false,
        capture_pageleave: false,
        autocapture: false,
        debug: process.env.MODE === "development",
        loaded: (posthogInstance) => {
          // Attach PostHog instance to window to follow the same pattern as other analytics tools
          window.posthog = posthogInstance;
        },
      }}
    >
      {children}
    </PostHogProvider>
  );
};
