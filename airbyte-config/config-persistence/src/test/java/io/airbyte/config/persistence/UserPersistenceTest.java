/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.data.services.impls.jooq.OAuthServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserPersistenceTest extends BaseConfigDatabaseTest {

  private UserPersistence userPersistence;

  @BeforeEach
  void beforeEach() throws Exception {
    userPersistence = new UserPersistence(database);
    truncateAllTables();
    setupTestData();
  }

  private void setupTestData() throws IOException, JsonValidationException {
    final ConfigRepository configRepository = new ConfigRepository(
        database,
        MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER,
        new WorkspaceServiceJooqImpl(database),
        new OrganizationServiceJooqImpl(database),
        new OAuthServiceJooqImpl(database));
    // write workspace table
    for (final StandardWorkspace workspace : MockData.standardWorkspaces()) {
      configRepository.writeStandardWorkspaceNoSecrets(workspace);
    }
    // write user table
    for (final User user : MockData.users()) {
      userPersistence.writeUser(user);
    }
  }

  @Test
  void getUserByIdTest() throws IOException {
    for (final User user : MockData.users()) {
      final Optional<User> userFromDb = userPersistence.getUser(user.getUserId());
      Assertions.assertEquals(user, userFromDb.get());
    }
  }

  @Test
  void getUserByAuthIdTest() throws IOException {
    for (final User user : MockData.users()) {
      final Optional<User> userFromDb = userPersistence.getUserByAuthId(user.getAuthUserId());
      Assertions.assertEquals(user, userFromDb.get());
    }
  }

  @Test
  void getUserByEmailTest() throws IOException {
    for (final User user : MockData.users()) {
      final Optional<User> userFromDb = userPersistence.getUserByEmail(user.getEmail());
      Assertions.assertEquals(user, userFromDb.get());
    }
  }

  @Test
  void deleteUserByIdTest() throws IOException {
    userPersistence.deleteUserById(MockData.CREATOR_USER_ID_1);
    Assertions.assertEquals(Optional.empty(), userPersistence.getUser(MockData.CREATOR_USER_ID_1));
  }

}
