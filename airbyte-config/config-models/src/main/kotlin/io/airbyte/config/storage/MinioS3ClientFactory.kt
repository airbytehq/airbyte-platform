package io.airbyte.config.storage

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.net.URI
import java.util.function.Supplier

class MinioS3ClientFactory(private val config: MinioStorageConfig) : Supplier<S3Client> {
  override fun get(): S3Client =
    runCatching {
      val minioUri = URI(config.endpoint)

      with(S3Client.builder()) {
        serviceConfiguration {
          it.pathStyleAccessEnabled(true)
        }
        credentialsProvider {
          AwsBasicCredentials.create(
            config.accessKey,
            config.secretAccessKey,
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
