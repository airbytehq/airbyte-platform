/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

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
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {

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
   * {@link NormalizeInDestinationHelperTest#testShouldNormalizeInDestination(List, String, boolean, boolean)}.
   */
  public static class ShouldNormalizeInDestinationArgumentsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          // Normalization not required
          Arguments.of(List.of(SOMETHING_ELSE), "destination-bigquery:1.3.2", true, false),
          // Container doesn't support it
          Arguments.of(List.of(NORMALIZATION_OPERATION), "hello:dev", true, false),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:1.3.0", true, false),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "hello:1.3.1", true, false),
          // Feature Flag off
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:dev", false, false),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:1.3.1", false, false),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:2.0.0", false, false),
          // Supported
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:dev", true, true),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:1.3.1", true, true),
          Arguments.of(List.of(NORMALIZATION_OPERATION), "destination-bigquery:2.0.0", true, true));
    }

  }

  @ParameterizedTest
  @ArgumentsSource(ShouldNormalizeInDestinationArgumentsProvider.class)
  void testShouldNormalizeInDestination(final List<StandardSyncOperation> syncOperations,
                                        final String imageName,
                                        final boolean featureFlagEnabled,
                                        final boolean expected) {
    final var actual = NormalizationInDestinationHelper.shouldNormalizeInDestination(syncOperations, imageName, featureFlagEnabled);
    Assertions.assertEquals(expected, actual);
  }

}
