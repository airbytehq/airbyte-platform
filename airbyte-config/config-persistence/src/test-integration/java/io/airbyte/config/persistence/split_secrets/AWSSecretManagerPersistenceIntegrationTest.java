/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.split_secrets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.config.Configs;
import io.airbyte.config.EnvConfigs;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class AWSSecretManagerPersistenceIntegrationTest {

  public String coordinateBase;
  private AWSSecretManagerPersistence persistence;

  @BeforeEach
  void setup() {
    coordinateBase = "aws/airbyte/secret/integration/" + RandomUtils.nextInt() % 20000;
  }

  private static Stream<Arguments> credentials() {
    Configs configs = new EnvConfigs();

    return Stream.of(
        Arguments.of(configs.getAwsAccessKey(), configs.getAwsSecretAccessKey()),
        Arguments.of(null, null));
  }

  @ParameterizedTest
  @MethodSource("credentials")
  void testReadWriteUpdate(String awsAccessKey, String awsSecretAccessKey) throws InterruptedException {
    SecretCoordinate secretCoordinate = new SecretCoordinate(coordinateBase, 1);
    persistence = new AWSSecretManagerPersistence(awsAccessKey, awsSecretAccessKey);

    // try reading a non-existent secret
    Optional<String> firstRead = persistence.read(secretCoordinate);
    assertTrue(firstRead.isEmpty());

    // write it
    String payload = "foo-secret";
    persistence.write(secretCoordinate, payload);
    persistence.cache.refreshNow(secretCoordinate.getCoordinateBase());
    Optional<String> read2 = persistence.read(secretCoordinate);
    assertTrue(read2.isPresent());
    assertEquals(payload, read2.get());

    // update it
    final var secondPayload = "bar-secret";
    final var coordinate2 = new SecretCoordinate(coordinateBase, 2);
    persistence.write(coordinate2, secondPayload);
    persistence.cache.refreshNow(secretCoordinate.getCoordinateBase());
    final var thirdRead = persistence.read(coordinate2);
    assertTrue(thirdRead.isPresent());
    assertEquals(secondPayload, thirdRead.get());
  }

  @AfterEach
  void tearDown() {
    persistence.deleteSecret(new SecretCoordinate(coordinateBase, 1));
  }

}
