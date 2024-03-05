/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.storage;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

/**
 * Takes in the constructor our standard format for gcs configuration and provides a factory that
 * uses that configuration to create a GCS client (Storage).
 */
@SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
public class DefaultGcsClientFactory implements Supplier<Storage> {

  private final GcsStorageConfig config;

  public DefaultGcsClientFactory(final GcsStorageConfig config) {
    this.config = config;
  }

  @Override
  public Storage get() {
    try {
      final var credentialsByteStream = new ByteArrayInputStream(Files.readAllBytes(Path.of(config.getApplicationCredentials())));
      final var credentials = ServiceAccountCredentials.fromStream(credentialsByteStream);
      return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

}
