import React, { useState, useEffect } from "react";

import { useWebappConfig } from "core/config";
import { trackError } from "core/utils/datadog";

declare global {
  interface Window {
    HockeyStack: HockeyStackAnalyticsObject;
  }
}

export interface HockeyStackAnalyticsObject {
  // https://docs.hockeystack.com/advanced-strategies-and-techniques/advanced-features/identifying-users
  identify: (identifier: string, customProperties?: Record<string, string | number | boolean>) => void;
}

const HOCKEYSTACK_SCRIPT_ID = "hs-snippet";

class HockeyStackLoadingError extends Error {}

export const HockeyStackAnalytics: React.FC<React.PropsWithChildren> = ({ children }) => {
  const { hockeystackApiKey } = useWebappConfig();
  const [hockeyStackInitialized, setHockeyStackInitialized] = useState(false);

  useEffect(() => {
    if (!hockeystackApiKey) {
      setHockeyStackInitialized(true);
      return;
    }

    const timeout = setTimeout(() => {
      setHockeyStackInitialized((initialized) => {
        if (!initialized) {
          trackError(new HockeyStackLoadingError("Script timed out"));
        }
        return true;
      });
    }, 10_000);

    if (!document.getElementById(HOCKEYSTACK_SCRIPT_ID)) {
      const script = document.createElement("script");
      script.id = HOCKEYSTACK_SCRIPT_ID;
      script.src = "https://cdn.jsdelivr.net/npm/hockeystack@1.3.191/hockeystack.min.js";
      // IMPORTANT: this SHA must be updated if we ever upgrade from version 1.3.191
      // Any updates to the HockeyStack snippet must be approved by security.
      script.integrity = "sha256-70jTH6ifyOB05/vjr6qwnaJQLCWdQFYl1xJZSsTdzQ4=";
      script.crossOrigin = "anonymous";
      script.dataset.cookieless = "1";
      script.dataset.apikey = hockeystackApiKey;
      script.onload = () => {
        setHockeyStackInitialized(true);
      };
      script.onerror = () => {
        trackError(new HockeyStackLoadingError("HockeyStack script onerror fired"));
        setHockeyStackInitialized(true);
      };
      document.body.appendChild(script);
    }

    return () => clearTimeout(timeout);
  }, [hockeystackApiKey]);

  // Don't render children until HockeyStack is loaded. This is important, because we want the
  // analyticsProvider.identify() call to only be fired once both Segment and HockeyStack are loaded.
  if (!hockeyStackInitialized) {
    return null;
  }

  return <>{children}</>;
};
