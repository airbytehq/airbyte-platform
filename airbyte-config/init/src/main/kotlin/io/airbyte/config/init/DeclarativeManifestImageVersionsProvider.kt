/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init

import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion

interface DeclarativeManifestImageVersionsProvider {
  fun getLatestDeclarativeManifestImageVersions(): List<DeclarativeManifestImageVersion>
}
