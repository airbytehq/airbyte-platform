/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.logging

import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.ComputeEngineCredentials
import com.google.auth.oauth2.CredentialAccessBoundary
import com.google.auth.oauth2.DownscopedCredentials
import com.google.auth.oauth2.GoogleCredentials
import io.airbyte.workload.api.domain.GcsDownscopedOAuthLogUploadAuthorization
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.time.ZoneOffset

interface LogUploadCredentialBroker {
  fun issue(
    bucketName: String,
    objectKeyPrefix: String,
  ): GcsDownscopedOAuthLogUploadAuthorization
}

@Singleton
open class ComputeEngineCredentialsProvider {
  open fun get(): GoogleCredentials = ComputeEngineCredentials.create()
}

@Singleton
open class GoogleDownscopedTokenProvider {
  open fun issue(
    sourceCredentials: GoogleCredentials,
    credentialAccessBoundary: CredentialAccessBoundary,
  ): AccessToken =
    DownscopedCredentials
      .newBuilder()
      .setSourceCredential(sourceCredentials)
      .setCredentialAccessBoundary(credentialAccessBoundary)
      .build()
      .refreshAccessToken()
}

@Singleton
class GcsLogUploadCredentialBroker(
  private val computeEngineCredentialsProvider: ComputeEngineCredentialsProvider,
  private val downscopedTokenProvider: GoogleDownscopedTokenProvider,
) : LogUploadCredentialBroker {
  override fun issue(
    bucketName: String,
    objectKeyPrefix: String,
  ): GcsDownscopedOAuthLogUploadAuthorization {
    val accessBoundary = credentialAccessBoundary(bucketName, objectKeyPrefix)
    val token =
      downscopedTokenProvider.issue(
        computeEngineCredentialsProvider.get(),
        accessBoundary,
      )
    val expiration = checkNotNull(token.expirationTime) { "GCS returned a token without an expiration." }

    return GcsDownscopedOAuthLogUploadAuthorization(
      accessToken = token.tokenValue,
      expiresAt = OffsetDateTime.ofInstant(expiration.toInstant(), ZoneOffset.UTC),
      bucketName = bucketName,
      objectKeyPrefix = objectKeyPrefix,
    )
  }

  private fun credentialAccessBoundary(
    bucketName: String,
    objectKeyPrefix: String,
  ): CredentialAccessBoundary {
    require(bucketName.isNotBlank()) { "GCS log bucket is not configured." }
    require(objectKeyPrefix.startsWith("job-logging/") && objectKeyPrefix.endsWith('/')) {
      "GCS log object prefix is invalid."
    }
    val objectResourcePrefix = "projects/_/buckets/$bucketName/objects/$objectKeyPrefix"
    val condition =
      CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition
        .newBuilder()
        .setExpression("resource.name.startsWith('${objectResourcePrefix.toCelString()}')")
        .build()
    val rule =
      CredentialAccessBoundary.AccessBoundaryRule
        .newBuilder()
        .setAvailableResource("//storage.googleapis.com/projects/_/buckets/$bucketName")
        .addAvailablePermission("inRole:roles/storage.objectCreator")
        .setAvailabilityCondition(condition)
        .build()

    return CredentialAccessBoundary.newBuilder().addRule(rule).build()
  }
}

private fun String.toCelString(): String = replace("\\", "\\\\").replace("'", "\\'")
