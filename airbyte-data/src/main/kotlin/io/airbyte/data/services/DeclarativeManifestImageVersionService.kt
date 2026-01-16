/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion

interface DeclarativeManifestImageVersionService {
  fun writeDeclarativeManifestImageVersion(declarativeManifestImageVersion: DeclarativeManifestImageVersion): DeclarativeManifestImageVersion

  fun getDeclarativeManifestImageVersionByMajorVersion(majorVersion: Int): DeclarativeManifestImageVersion

  fun listDeclarativeManifestImageVersions(): List<DeclarativeManifestImageVersion>
}
