// Add at the very top of your server.js file:
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const path = require("path");

// eslint-disable-next-line @typescript-eslint/no-var-requires
const express = require("express");

const app = express();
const PORT = process.env.PORT || 4000;

app.use(express.json());

// CORS for local widget API calls
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

// Serve index.html
app.get("/index.html", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

// Serve static files if needed (e.g., CSS, JS)
// app.use(express.static(__dirname));

// Read config from environment variables
const BASE_URL = process.env.BASE_URL;
const AIRBYTE_WIDGET_URL = `${BASE_URL}/v1/embedded/widget_token`;
const AIRBYTE_ACCESS_TOKEN_URL = `${BASE_URL}/v1/applications/token`;
const AIRBYTE_CLIENT_ID = process.env.AIRBYTE_CLIENT_ID;
const AIRBYTE_CLIENT_SECRET = process.env.AIRBYTE_CLIENT_SECRET;
const ORGANIZATION_ID = process.env.AIRBYTE_ORGANIZATION_ID;
const EXTERNAL_USER_ID = process.env.EXTERNAL_USER_ID;

// Proxy /api/widget_token to real Airbyte API
app.get("/api/widget_token", async (req, res) => {
  try {
    // Determine the allowed origin from the request
    let origin = req.headers.origin;
    if (!origin && req.headers.referer) {
      try {
        origin = new URL(req.headers.referer).origin;
      } catch {
        origin = "";
      }
    }

    const access_token_body = JSON.stringify({
      client_id: AIRBYTE_CLIENT_ID,
      client_secret: AIRBYTE_CLIENT_SECRET,
      "grant-type": "client_credentials",
    });

    console.log("[/api/widget_token] Requesting application token", {
      AIRBYTE_ACCESS_TOKEN_URL,
      access_token_body,
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
    });

    const response = await fetch(AIRBYTE_ACCESS_TOKEN_URL, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: access_token_body,
    });

    console.log("[/api/widget_token] Access token response status", response.status, response.statusText);

    if (!response.ok) {
      const errorText = await response.text();
      console.error("[/api/widget_token] Error response from Airbyte:", errorText);
      return res.status(500).json({ error: "Failed to fetch access token" });
    }

    const access_token_response = await response.json();
    const access_token = access_token_response.access_token;

    console.log("[/api/widget_token] Access token received:", access_token);

    console.log("[/api/widget_token] Requesting widget token", {
      AIRBYTE_WIDGET_URL,
      body: JSON.stringify({
        organizationId: ORGANIZATION_ID,
        allowedOrigin: origin,
        externalUserId: EXTERNAL_USER_ID,
      }),
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
    });

    const widget_token_response = await fetch(AIRBYTE_WIDGET_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${access_token}`,
      },
      body: JSON.stringify({
        organizationId: ORGANIZATION_ID,
        allowedOrigin: origin,
        externalUserId: EXTERNAL_USER_ID,
      }),
    });

    if (!widget_token_response.ok) {
      const errorText = await widget_token_response.text();
      console.error("Error response from Airbyte:", errorText);
      return res.status(500).json({ error: "Failed to fetch embedded token" });
    }

    const widget_response = await widget_token_response.json();

    res.json({ token: widget_response.token });
  } catch (err) {
    console.error("Unexpected error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Mock /api/embedded_response endpoint
app.post("/api/embedded_response", (req, res) => {
  if (req.body.message === "partial_user_config_created") {
    console.log("EMBEDDED_RESPONSE_RECEIVED");
    res.status(200).send("OK");
  } else {
    console.error("Unexpected message:", req.body.message);
    res.status(400).send(req.body.message);
  }
});

const server = app.listen(PORT, () => {
  console.log(`Embedded test server running at http://localhost:${PORT}/index.html`);
});

server.on("error", (err) => {
  if (err.code === "EADDRINUSE") {
    console.error(`Port ${PORT} is already in use. Please free the port and try again.`);
    process.exit(1);
  } else {
    console.error("Server error:", err);
    process.exit(1);
  }
});
