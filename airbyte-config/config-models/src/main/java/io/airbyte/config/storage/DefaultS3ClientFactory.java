/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.storage;

import java.util.function.Supplier;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Takes in the constructor our standard format for S3 configuration and provides a factory that
 * uses that configuration to create an S3Client.
 */
public class DefaultS3ClientFactory implements Supplier<S3Client> {

  private final S3StorageConfig config;

  public DefaultS3ClientFactory(final S3StorageConfig config) {
    this.config = config;
  }

  @Override
  public S3Client get() {
    final var builder = S3Client.builder();

    // If credentials are part of this config, specify them. Otherwise,
    // let the SDK's default credential provider take over.
    if (config.getAccessKey() != null && !config.getAccessKey().isEmpty()) {
      builder.credentialsProvider(() -> AwsBasicCredentials.create(config.getAccessKey(), config.getSecretAccessKey()));
    }
    builder.region(Region.of(config.getRegion()));
    return builder.build();
  }

}
