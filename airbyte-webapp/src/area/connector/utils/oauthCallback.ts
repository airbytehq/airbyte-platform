/**
 * Important: This file is the entry point for the oauth-callback.html file. This is a separately built
 * html page that will serve as the redirect_uri for OAuth connectors. It's only job is to inform the opening tab
 * about the query parameters it gets. Thus this page should be as small as possible and this file should ideally import
 * from as few files as possible. Also note that this file isn't inside the React app.
 */

import type { OAuthEvent } from "../types/oauthCallback";

import { BroadcastChannel } from "broadcast-channel";

import { OAUTH_BROADCAST_CHANNEL_NAME } from "./oauthConstants";
import { extractCustomerRedirectUrl } from "./oauthStateUtils";

// Get the current query parameters and send them to the parent tab via a broadcast channel.
// The code in useConnectorAuth is listening for this message.
const search = new URLSearchParams(window.location.search);
const query = Object.fromEntries(search);

const customerRedirectUrl = extractCustomerRedirectUrl(query.state || null);

// If there's a customer redirect URL, redirect to it with the OAuth params
// This handles cross-origin OAuth flows (e.g., Shopify OAuth initiated from Sonar)
if (customerRedirectUrl) {
  const redirectUrl = new URL("/auth_flow", customerRedirectUrl);
  // Forward all query params except state (we'll send a cleaned state)
  for (const [key, value] of Object.entries(query)) {
    if (key === "state") {
      // Send only the random state part, not the redirect URL
      const cleanState = value.split("|")[0];
      redirectUrl.searchParams.set(key, cleanState);
    } else {
      redirectUrl.searchParams.set(key, value);
    }
  }
  window.location.href = redirectUrl.toString();
} else {
  // Standard same-origin flow: use postMessage and BroadcastChannel

  // Try to send via postMessage first (for Embedded context)
  // Only send if opener exists and is from our domain
  if (window.opener && window.opener.location.origin === window.location.origin) {
    window.opener.postMessage({ type: "completed", query }, window.location.origin);
  }

  // Also send via broadcast channel (for non-embedded context)
  const bc = new BroadcastChannel<OAuthEvent>(OAUTH_BROADCAST_CHANNEL_NAME);
  bc.postMessage({ type: "completed", query });

  bc.onmessage = async (event) => {
    if (event.type === "close") {
      window.close();
    }
  };

  // Wait to receive the "close" event from the original tab.
  // This is necessary because Shopify requires clicking the Open app button to open our app homepage, rather than just close out immediately.
  // To satisfy this requirement, we wait 5 seconds in this /auth_flow page to receive the "close" event, which indicates that the user initiated
  // this from the Airbyte UI rather than the Shopify app.
  // If we don't receive it, redirect to the index page to satisfy Shopify's requirements.
  // See notion page for more details: https://www.notion.so/Shopify-App-Listing-Requirements-1f51b3df260c8029b2a7de0399c8d5d2
  setTimeout(() => {
    window.location.href = "/";
  }, 5000);
}
