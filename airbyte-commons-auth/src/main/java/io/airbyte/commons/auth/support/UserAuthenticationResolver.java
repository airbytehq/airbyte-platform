/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.support;

import io.airbyte.config.AuthenticatedUser;
import java.util.Optional;

/**
 * Interface for resolving user authentication attributes into an Airbyte User object.
 */
public interface UserAuthenticationResolver {

  AuthenticatedUser resolveUser(final String expectedAuthUserId);

  Optional<String> resolveRealm();

}
