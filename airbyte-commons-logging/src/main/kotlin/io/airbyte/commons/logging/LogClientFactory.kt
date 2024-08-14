/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.io.ByteArrayInputStream
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.util.function.Supplier

/**
 * Defines a log client factory used to create a new client to the logging provider.
 */
interface LogClientFactory<T> : Supplier<T>

@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^gcs$")
@Named("gcsLogClientFactory")
class GcsLogClientFactory(
  @Value("\${$STORAGE_GCS.application-credentials}") val applicationCredentials: String,
) : LogClientFactory<Storage> {
  override fun get(): Storage {
    runCatching {
      val credentialsByteStream = ByteArrayInputStream(Files.readAllBytes(Path.of(applicationCredentials)))
      val credentials = ServiceAccountCredentials.fromStream(credentialsByteStream)
      return StorageOptions.newBuilder().setCredentials(credentials).build().service
    }.getOrElse {
      throw RuntimeException("Error creating GCS log client", it)
    }
  }
}

@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^minio$")
@Named("minioLogClientFactory")
class MinioS3LogClientFactory(
  @Value("\${$STORAGE_MINIO.access-key}") val accessKey: String,
  @Value("\${$STORAGE_MINIO.secret-access-key}") val secretAccessKey: String,
  @Value("\${$STORAGE_MINIO.endpoint}") val endpoint: String,
) : LogClientFactory<S3Client> {
  override fun get(): S3Client =
    runCatching {
      val minioUri = URI(endpoint)

      with(S3Client.builder()) {
        serviceConfiguration {
          it.pathStyleAccessEnabled(true)
        }
        credentialsProvider {
          AwsBasicCredentials.create(
            accessKey,
            secretAccessKey,
          )
        }
        endpointOverride(minioUri)
        // Although this is not used, the S3 client will error out if this is not set.
        region(Region.US_EAST_1)
        build()
      }
    }.getOrElse {
      throw RuntimeException("Error creating S3 log client to Minio", it)
    }
}

@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^s3$")
@Named("s3LogClientFactory")
class S3LogClientFactory(
  @Value("\${$STORAGE_S3.access-key}") val accessKey: String,
  @Value("\${$STORAGE_S3.secret-access-key}") val secretAccessKey: String,
  @Value("\${$STORAGE_S3.region}") val region: String,
) : LogClientFactory<S3Client> {
  override fun get(): S3Client {
    runCatching {
      val builder = S3Client.builder()
      // If credentials are part of this config, specify them. Otherwise,
      // let the SDK's default credential provider take over.
      if (accessKey.isNotEmpty()) {
        builder.credentialsProvider {
          AwsBasicCredentials.create(
            accessKey,
            secretAccessKey,
          )
        }
      }
      builder.region(Region.of(region))
      return builder.build()
    }.getOrElse {
      throw RuntimeException("Error creating S3 log client", it)
    }
  }
}
