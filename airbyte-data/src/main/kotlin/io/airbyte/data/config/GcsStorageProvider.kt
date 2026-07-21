/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.config

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.nio.file.Files
import java.nio.file.Path

private val logger = KotlinLogging.logger {}

/**
 * Builds a GCS [Storage] client from a service-account credentials file.
 */
@Singleton
class GcsStorageProvider {
  fun provideStorage(
    gcsCredentialsFilepath: String,
    gcsProjectId: String,
  ): Storage {
    val credentials = GoogleCredentials.fromStream(Files.newInputStream(Path.of(gcsCredentialsFilepath)))
    val storage =
      StorageOptions
        .newBuilder()
        .setCredentials(credentials)
        .setProjectId(gcsProjectId)
        .build()
        .service

    logger.info { "Initialized GCS storage for project $gcsProjectId" }

    return storage
  }
}
