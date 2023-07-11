/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.helper;

import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Unit Test class for {@link NormalizationInDestinationHelper}.
 */
class NormalizeInDestinationHelperTest {

  private static final StandardSyncOperation NORMALIZATION_OPERATION =
      new StandardSyncOperation().withOperatorType(OperatorType.NORMALIZATION);

  private static final StandardSyncOperation SOMETHING_ELSE =
      new StandardSyncOperation().withOperatorType(OperatorType.WEBHOOK);

  /**
   * Argument provider for
   * {@link NormalizeInDestinationHelperTest#testNormalizationStepRequired(List, boolean)}.
   */
  public static class NormalizeStepRequiredArgumentsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {

      return Stream.of(
          Arguments.of(List.of(SOMETHING_ELSE), false),
          Arguments.of(List.of(SOMETHING_ELSE, NORMALIZATION_OPERATION), true));
    }

  }

  @ParameterizedTest
  @ArgumentsSource(NormalizeStepRequiredArgumentsProvider.class)
  void testNormalizationStepRequired(final List<StandardSyncOperation> standardSyncOperations, final boolean expected) {
    final var actual = NormalizationInDestinationHelper.normalizationStepRequired(standardSyncOperations);
    Assertions.assertEquals(expected, actual);
  }

  @Test
  void testGetAdditionalEnvironmentVariables() {
    final var shouldBeEmpty = NormalizationInDestinationHelper.getAdditionalEnvironmentVariables(false);
    Assertions.assertTrue(shouldBeEmpty.isEmpty());
    final var shouldBePopulated = NormalizationInDestinationHelper.getAdditionalEnvironmentVariables(true);
    Assertions.assertFalse(shouldBePopulated.isEmpty());
  }

  /**
   * Argument provider for
   * {@link NormalizeInDestinationHelperTest#testShouldNormalizeInDestination(List, String, String, boolean)}.
   */
  public static class ShouldNormalizeInDestinationArgumentsProvider implements ArgumentsProvider {

    // for testing only
    private static final String MIN_SUPPORTED_VERSION_OFF = "";
    private static final String MIN_SUPPORTED_VERSION_BIGQUERY = "1.3.1";
    private static final String MIN_SUPPORTED_VERSION_SNOWFLAKE = "1.0.0";

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
      return Stream.of(
          // Normalization not required
          Arguments.of(List.of(SOMETHING_ELSE), "destination-bigquery:1.3.2", MIN_SUPPORTED_VERSION_BIGQUERY, false),
          // Container doesn't support it
          Arguments.of(List.of(NORMALIZATION_OPERATION), "hello:dev", MIN_SUPPORTED_VERSION_OFF, false),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "hello:1.3.1", MIN_SUPPORTED_VERSION_OFF, false),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:1.3.0", MIN_SUPPORTED_VERSION_BIGQUERY, false),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-snowflake:0.0.0", MIN_SUPPORTED_VERSION_SNOWFLAKE, false),
          // Feature Flag off
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:dev", MIN_SUPPORTED_VERSION_OFF, false),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:1.3.1", MIN_SUPPORTED_VERSION_OFF, false),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:2.0.0", MIN_SUPPORTED_VERSION_OFF, false),
          // Supported
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:dev", MIN_SUPPORTED_VERSION_BIGQUERY, true),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:1.3.1", MIN_SUPPORTED_VERSION_BIGQUERY, true),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:2.0.0", MIN_SUPPORTED_VERSION_BIGQUERY, true),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-snowflake:2.0.0", MIN_SUPPORTED_VERSION_SNOWFLAKE, true));
    }

  }

  @ParameterizedTest
  @ArgumentsSource(ShouldNormalizeInDestinationArgumentsProvider.class)
  void testShouldNormalizeInDestination(final List<StandardSyncOperation> syncOperations,
                                        final String imageName,
                                        final String minSupportedVersion,
                                        final boolean expected) {
    final var actual = NormalizationInDestinationHelper.shouldNormalizeInDestination(syncOperations, imageName, minSupportedVersion);
    Assertions.assertEquals(expected, actual);
  }

}
