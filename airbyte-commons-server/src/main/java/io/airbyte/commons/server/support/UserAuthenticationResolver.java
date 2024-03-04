/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.airbyte.config.User;
import java.util.Optional;

/**
 * Interface for resolving user authentication attributes into an Airbyte User object.
 */
public interface UserAuthenticationResolver {

  User resolveUser(final String expectedAuthUserId);

  Optional<String> resolveSsoRealm();

}
