/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validation;

import io.airbyte.commons.server.errors.ApplicationErrorKnownException;
import java.util.UUID;

/**
 * Interface for validating access to actor definitions. Implementations vary across Self-Hosted and
 * Cloud editions.
 */
public interface ActorDefinitionAccessValidator {

  /**
   * Check if the current user/request has write access to the indicated actor definition.
   *
   * @param actorDefinitionId the primary key ID of the actor definition to check
   * @throws ApplicationErrorKnownException if the user does not have write access to the actor
   *         definition
   */
  void validateWriteAccess(final UUID actorDefinitionId) throws ApplicationErrorKnownException;

}
