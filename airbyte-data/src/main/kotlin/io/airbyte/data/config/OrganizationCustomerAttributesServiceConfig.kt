/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.config

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import java.nio.file.Files
import java.nio.file.Paths

private val logger = KotlinLogging.logger {}

@Factory
@Named("customerTierStorage")
class OrganizationCustomerAttributesServiceConfig {
  fun provideStorage(
    gcsApplicationCredentials: String?,
    gcsProjectId: String?,
  ): Storage? {
    if (gcsApplicationCredentials.isNullOrBlank()) {
      logger.info {
        "Cannot initialize storage for OrganizationCustomerAttributesService;" +
          "gcsProjectId=$gcsProjectId gcsApplicationCredentials=$gcsApplicationCredentials"
      }

      return null
    }
    val credentials = GoogleCredentials.fromStream(Files.newInputStream(Paths.get(gcsApplicationCredentials)))
    val storage =
      StorageOptions
        .newBuilder()
        .setCredentials(credentials)
        .setProjectId(gcsProjectId)
        .build()
        .service

    logger.info {
      "Initialized storage for OrganizationCustomerAttributesService for project $gcsProjectId"
    }

    return storage
  }
}
