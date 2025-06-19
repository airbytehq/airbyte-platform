/// <reference types="@types/segment-analytics" />

import React, { useRef } from "react";

import { useWebappConfig } from "core/config";

interface AnalyticsProviderProps {
  disableSegment?: boolean;
}

export const AnalyticsProvider: React.FC<React.PropsWithChildren<AnalyticsProviderProps>> = React.memo(
  ({ children, disableSegment }) => {
    const segmentScriptLoadedRef = useRef(false);
    const { segmentToken } = useWebappConfig();

    // We want this to synchronously load once before {children} is rendered. Otherwise some calls to the AnalyticsService might execute before the Segment script is loaded.
    if (!segmentScriptLoadedRef.current) {
      segmentScriptLoadedRef.current = true;
      if (segmentToken && !disableSegment) {
        const script = document.createElement("script");
        script.innerText = `
          !function(){var i="analytics",analytics=window[i]=window[i]||[];if(!analytics.initialize)if(analytics.invoked)window.console&&console.error&&console.error("Segment snippet included twice.");else{analytics.invoked=!0;analytics.methods=["trackSubmit","trackClick","trackLink","trackForm","pageview","identify","reset","group","track","ready","alias","debug","page","screen","once","off","on","addSourceMiddleware","addIntegrationMiddleware","setAnonymousId","addDestinationMiddleware","register"];analytics.factory=function(e){return function(){if(window[i].initialized)return window[i][e].apply(window[i],arguments);var n=Array.prototype.slice.call(arguments);if(["track","screen","alias","group","page","identify"].indexOf(e)>-1){var c=document.querySelector("link[rel='canonical']");n.push({__t:"bpc",c:c&&c.getAttribute("href")||void 0,p:location.pathname,u:location.href,s:location.search,t:document.title,r:document.referrer})}n.unshift(e);analytics.push(n);return analytics}};for(var n=0;n<analytics.methods.length;n++){var key=analytics.methods[n];analytics[key]=analytics.factory(key)}analytics.load=function(key,n){var t=document.createElement("script");t.type="text/javascript";t.async=!0;t.setAttribute("data-global-segment-analytics-key",i);t.src="https://cdn.segment.com/analytics.js/v1/" + key + "/analytics.min.js";var r=document.getElementsByTagName("script")[0];r.parentNode.insertBefore(t,r);analytics._loadOptions=n};analytics._writeKey="${segmentToken}";;analytics.SNIPPET_VERSION="5.2.1";
          analytics.load("${segmentToken}");
          analytics.page();
          }}();
      `;

        if (typeof window !== "undefined" && !window.analytics) {
          document.body.appendChild(script);
        }
      }
    }

    return <>{children}</>;
  }
);

AnalyticsProvider.displayName = "AnalyticsProvider";
