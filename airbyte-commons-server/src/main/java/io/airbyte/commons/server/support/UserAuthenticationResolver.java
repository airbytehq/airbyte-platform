/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.airbyte.config.User;

/**
 * Interface for resolving user authentication attributes into an Airbyte User object.
 */
public interface UserAuthenticationResolver {

  User resolveUser(final String expectedAuthUserId);

  String resolveSsoRealm();

}
