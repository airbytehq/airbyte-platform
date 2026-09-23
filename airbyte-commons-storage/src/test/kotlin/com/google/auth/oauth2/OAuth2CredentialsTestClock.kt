/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package com.google.auth.oauth2

import com.google.api.client.util.Clock

/** Installs a deterministic clock through the package-private seam available in google-auth 1.39.1. */
internal fun OAuth2CredentialsWithRefresh.useTestClock(clock: Clock) {
  this.clock = clock
}
