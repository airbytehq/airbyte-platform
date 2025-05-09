/**
 * Important: This file is the entry point for the oauth-callback.html file. This is a separately built
 * html page that will serve as the redirect_uri for OAuth connectors. It's only job is to inform the opening tab
 * about the query parameters it gets. Thus this page should be as small as possible and this file should ideally import
 * from as few files as possible. Also note that this file isn't inside the React app.
 */

import type { OAuthEvent } from "../types/oauthCallback";

import { BroadcastChannel } from "broadcast-channel";

import { OAUTH_BROADCAST_CHANNEL_NAME } from "./oauthConstants";

// Get the current query parameters and send them to the parent tab via a broadcast channel.
// The code in useConnectorAuth is listening for this message.
const search = new URLSearchParams(window.location.search);
const query = Object.fromEntries(search);

// Try to send via postMessage first (for Embedded context)
// Only send if opener exists and is from our domain
if (window.opener && window.opener.location.origin === window.location.origin) {
  window.opener.postMessage({ type: "completed", query }, window.location.origin);
}

// Also send via broadcast channel (for non-embedded context)
const bc = new BroadcastChannel<OAuthEvent>(OAUTH_BROADCAST_CHANNEL_NAME);
bc.postMessage({ type: "completed", query });

const redirectToIndex = "redirect_to_index" in query ? true : false;
// Close popup window once we're done
if (redirectToIndex) {
  // Redirect to the index page to support Shopify's strange integration requirements
  // see https://shopify.dev/docs/apps/launch/app-requirements-checklist#b-permissions for more details
  window.location.href = "/";
} else {
  window.close();
}
