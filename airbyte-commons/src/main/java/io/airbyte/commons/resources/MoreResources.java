/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.resources;

import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import io.airbyte.commons.lang.Exceptions;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Stream;

/**
 * Common code for operations on a java Resource.
 */
public class MoreResources {

  /**
   * Read a java Resource entirely into a string.
   *
   * @param resourceName name of resource
   * @return contents of the resource as a string
   * @throws IOException throw if failure while reading resource
   */
  public static String readResource(final String resourceName) throws IOException {
    final URL resource = Resources.getResource(resourceName);
    return Resources.toString(resource, StandardCharsets.UTF_8);
  }

  /**
   * Read a java Resource entirely into a string.
   *
   * @param klass class that resource is attached to
   * @param resourceName name of resource
   * @return contents of the resource as a string
   * @throws IOException throw if failure while reading resource
   */
  public static String readResource(final Class<?> klass, final String resourceName) throws IOException {
    final String rootedName = !resourceName.startsWith("/") ? String.format("/%s", resourceName) : resourceName;
    final URL url = Resources.getResource(klass, rootedName);
    return Resources.toString(url, StandardCharsets.UTF_8);
  }

  /**
   * Access a java Resource as a file.
   *
   * @param resourceName name of resource
   * @return file handle to the Resource
   * @throws URISyntaxException throw if failure while creating file handle
   */
  public static File readResourceAsFile(final String resourceName) throws URISyntaxException {
    return new File(Resources.getResource(resourceName).toURI());
  }

  /**
   * This class is a bit of a hack. Might have unexpected behavior.
   *
   * @param klass class whose resources will be access
   * @param name path to directory in resources list
   * @return stream of paths to each resource file. THIS STREAM MUST BE CLOSED.
   * @throws IOException you never know when you IO.
   */
  public static Stream<Path> listResources(final Class<?> klass, final String name) throws IOException {
    Preconditions.checkNotNull(klass);
    Preconditions.checkNotNull(name);
    Preconditions.checkArgument(!name.isBlank());

    try {
      final String rootedResourceDir = !name.startsWith("/") ? String.format("/%s", name) : name;
      final URL url = klass.getResource(rootedResourceDir);
      // noinspection ConstantConditions
      Preconditions.checkNotNull(url, "Could not find resource.");

      final Path searchPath;
      if (url.toString().startsWith("jar")) {
        final FileSystem fileSystem = FileSystems.newFileSystem(url.toURI(), Collections.emptyMap());
        searchPath = fileSystem.getPath(rootedResourceDir);
        return Files.walk(searchPath, 1).onClose(() -> Exceptions.toRuntime(fileSystem::close));
      } else {
        searchPath = Path.of(url.toURI());
        return Files.walk(searchPath, 1);
      }

    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

}
