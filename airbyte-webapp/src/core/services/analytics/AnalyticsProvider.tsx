/// <reference types="@types/segment-analytics" />

import React from "react";
import { useEffectOnce } from "react-use";

import { config } from "core/config";

export const AnalyticsProvider: React.FC<React.PropsWithChildren> = React.memo(({ children }) => {
  useEffectOnce(() => {
    if (config.segment.enabled && config.segment.token) {
      const script = document.createElement("script");
      script.innerText = `
      !function(){var analytics=window.analytics=window.analytics||[];if(!analytics.initialize)if(analytics.invoked)window.console&&console.error&&console.error("Segment snippet included twice.");else{analytics.invoked=!0;analytics.methods=["trackSubmit","trackClick","trackLink","trackForm","pageview","identify","reset","group","track","ready","alias","debug","page","once","off","on","addSourceMiddleware","addIntegrationMiddleware","setAnonymousId","addDestinationMiddleware"];analytics.factory=function(e){return function(){var t=Array.prototype.slice.call(arguments);t.unshift(e);analytics.push(t);return analytics}};for(var e=0;e<analytics.methods.length;e++){var key=analytics.methods[e];analytics[key]=analytics.factory(key)}analytics.load=function(key,e){var t=document.createElement("script");t.type="text/javascript";t.async=!0;t.src="https://seg.airbyte.com/analytics.js/v1/" + key + "/analytics.min.js";var n=document.getElementsByTagName("script")[0];n.parentNode.insertBefore(t,n);analytics._loadOptions=e};analytics._writeKey="${config.segment.token}";analytics._cdn="https://seg.airbyte.com";analytics.SNIPPET_VERSION="4.15.3";
        analytics.load("${config.segment.token}");
        analytics.page();
      }}();
      `;
      script.async = true;

      if (typeof window !== "undefined" && !window.analytics) {
        document.body.appendChild(script);
      }
    }
  });

  return <>{children}</>;
});

AnalyticsProvider.displayName = "AnalyticsProvider";
