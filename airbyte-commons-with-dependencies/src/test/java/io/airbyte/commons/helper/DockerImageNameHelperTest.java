/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.helper;

import io.airbyte.commons.version.Version;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Unit Test class for {@link DockerImageNameHelper}.
 */
class DockerImageNameHelperTest {

  @ParameterizedTest
  @CsvSource({"hello:1,hello", "hello/world:2,world", "foo/bar/fizz/buzz:3,buzz", "hello,hello", "registry.internal:1234/foo/bar:1,bar"})
  void testExtractShortImageName(final String fullName, final String expected) {
    final var actual = DockerImageNameHelper.extractShortImageName(fullName);
    Assertions.assertEquals(expected, actual);
  }

  @ParameterizedTest
  @CsvSource({"hello:1,hello", "hello/world:2,hello/world", "foo/bar/fizz/buzz:3,foo/bar/fizz/buzz", "hello,hello", "hello:1.1-foo,hello",
    "registry.internal:1234/foo/bar:1,registry.internal:1234/foo/bar"})
  void testExtractImageNameWithoutVersion(final String fullName, final String expected) {
    final var actual = DockerImageNameHelper.extractImageNameWithoutVersion(fullName);
    Assertions.assertEquals(expected, actual);
  }

  @ParameterizedTest
  @CsvSource({"hello:1,1", "hello/world:2,2", "foo/bar/fizz/buzz:3,3", "hello,", "hello:1.1-foo,1.1-foo", "registry.internal:1234/foo/bar:1,1"})
  void testExtractImageVersionString(final String fullName, final String expected) {
    final var actual = DockerImageNameHelper.extractImageVersionString(fullName);
    Assertions.assertEquals(expected, actual);
  }

  static class ExtractImageVersionArgumentsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of("hello:1.1.1", new Version("1.1.1")),
          Arguments.of("hello:1", null),
          Arguments.of("hello:dev", new Version("dev")),
          Arguments.of("hello", null),
          Arguments.of("hello/foo/bar:1.2.3", new Version("1.2.3")),
          Arguments.of("registry.internal:1234/foo/bar:1.0.0", new Version("1.0.0")));
    }

  }

  @ParameterizedTest
  @ArgumentsSource(ExtractImageVersionArgumentsProvider.class)
  void testExtractImageVersion(final String fullName, final Version expected) {
    final var actual = DockerImageNameHelper.extractImageVersion(fullName);
    if (actual.isPresent()) {
      Assertions.assertEquals(0, expected.compatibleVersionCompareTo(actual.get()));
    } else {
      Assertions.assertNull(expected);
    }
  }

}
