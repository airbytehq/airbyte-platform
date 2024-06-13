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
const bc = new BroadcastChannel<OAuthEvent>(OAUTH_BROADCAST_CHANNEL_NAME);
bc.postMessage({ type: "completed", query });
// Close popup window once we're done
window.close();
