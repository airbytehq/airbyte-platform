/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init

import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("localDeclarativeManifestImageVersionsProvider")
class LocalDeclarativeManifestImageVersionsProvider : DeclarativeManifestImageVersionsProvider {
  override fun getLatestDeclarativeManifestImageVersions(): List<DeclarativeManifestImageVersion> =
    listOf(
      DeclarativeManifestImageVersion(0, "0.90.0", "sha256:d4b897be4f4c9edc5073b60f625cadb6853d8dc7e6178b19c414fe9b743fde33"),
      DeclarativeManifestImageVersion(1, "1.7.0", "sha256:3cd10771ef1608e8a637ec54a3e5e55760953e2bb187bb0ea4851b869c61258d"),
      DeclarativeManifestImageVersion(2, "2.1.0", "sha256:1e0df4ded4ea1e75b762872cb44c2f62458b1851dc76601cb733400465b790d0"),
      DeclarativeManifestImageVersion(3, "3.10.4", "sha256:aded5b43dfa140f18c55777205bf654690399c3cf0a51aaf26218eba75274eb5"),
      DeclarativeManifestImageVersion(4, "4.3.0", "sha256:538af3f6237799f10458b2b7bb5d69889c4ebae2b7d82b84b7b887fde9c53aa4"),
    )
}
