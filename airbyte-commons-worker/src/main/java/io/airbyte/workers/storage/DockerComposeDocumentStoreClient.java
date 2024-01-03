/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.storage;

import io.airbyte.commons.io.IOs;
import io.airbyte.config.storage.CloudStorageConfigs.LocalConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Document store for when all we have is an FS. This should only be used in the docker-compose
 * case. Leverages the workspace mount as a storage area.
 */
public class DockerComposeDocumentStoreClient implements DocumentStoreClient {

  private final Path workspaceMount;

  public static DockerComposeDocumentStoreClient create(final LocalConfig config, final Path prefix) {
    // This is a trick to append prefix to root as Path.concat doesn't exist.
    final File rootFile = new File(config.getRoot());
    final File prefixFile = new File(rootFile, prefix.toString());
    return new DockerComposeDocumentStoreClient(prefixFile.toPath());
  }

  public DockerComposeDocumentStoreClient(final Path workspaceMount) {
    this.workspaceMount = workspaceMount;
  }

  private Path getRoot() {
    return workspaceMount;
  }

  private Path getPath(final String id) {
    return getRoot().resolve(id);
  }

  @Override
  public void write(final String id, final String document) {
    final Path path = getPath(id);
    createDirectoryWithParents(path.getParent());
    IOs.writeFile(path, document);
  }

  private void createDirectoryWithParents(final Path path) {
    try {
      Files.createDirectories(path);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<String> read(final String id) {
    final Path path = getPath(id);
    if (Files.exists(path)) {
      return Optional.ofNullable(IOs.readFile(path));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public boolean delete(final String id) {
    final Path path = getPath(id);
    try {
      return Files.deleteIfExists(path);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

}
