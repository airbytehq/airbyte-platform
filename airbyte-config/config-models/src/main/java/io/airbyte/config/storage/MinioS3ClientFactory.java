/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Supplier;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * When using minio, we can still leverage the S3Client, we just slightly change what information we
 * pass to it. Takes in the constructor our standard format for minio configuration and provides a
 * factory that uses that configuration to create an S3Client.
 */
@SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
public class MinioS3ClientFactory implements Supplier<S3Client> {

  private final MinioStorageConfig config;

  public MinioS3ClientFactory(final MinioStorageConfig config) {
    this.config = config;
  }

  @Override
  public S3Client get() {
    final var builder = S3Client.builder();

    // The Minio S3 client.
    final var minioEndpoint = config.getEndpoint();
    try {
      final var minioUri = new URI(minioEndpoint);
      builder.credentialsProvider(() -> AwsBasicCredentials.create(config.getAccessKey(), config.getSecretAccessKey()));
      builder.endpointOverride(minioUri);
      builder.region(Region.US_EAST_1); // Although this is not used, the S3 client will error out if this is not set. Set a stub value.
    } catch (final URISyntaxException e) {
      throw new RuntimeException("Error creating S3 log client to Minio", e);
    }

    return builder.build();
  }

}
