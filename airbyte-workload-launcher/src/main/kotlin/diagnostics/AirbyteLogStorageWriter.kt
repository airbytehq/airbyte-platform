/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.StorageOptions
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.GcsStorageClient
import io.airbyte.workload.api.domain.GcsDownscopedOAuthLogUploadAuthorization
import io.airbyte.workload.api.domain.LogUploadAuthorization
import jakarta.inject.Singleton
import java.util.Date

/**
 * Writes a file to Airbyte's log bucket with the upload authorization Airbyte issued for this data plane.
 */
@Singleton
class AirbyteLogStorageWriter {
  fun write(
    authorization: LogUploadAuthorization,
    fileName: String,
    contents: String,
  ) = when (authorization) {
    is GcsDownscopedOAuthLogUploadAuthorization -> {
      val credentials = GoogleCredentials.create(AccessToken(authorization.accessToken, Date.from(authorization.expiresAt.toInstant())))
      StorageOptions.newBuilder().setCredentials(credentials).build().service.use { gcs ->
        GcsStorageClient
          .fromPrebuiltClient(authorization.bucketName, DocumentType.LOGS, gcs)
          .write(authorization.objectKeyPrefix + fileName, contents)
      }
    }
  }
}
