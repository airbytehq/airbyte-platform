/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validation;

import io.airbyte.commons.server.errors.ApplicationErrorKnownException;
import jakarta.inject.Singleton;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Default Community edition implementation of {@link ActorDefinitionAccessValidator}. Does nothing,
 * because Community edition does not have any access restrictions/auth.
 */
@Singleton
@Slf4j
public class CommunityActorDefinitionAccessValidator implements ActorDefinitionAccessValidator {

  @Override
  public void validateWriteAccess(final UUID actorDefinitionId) throws ApplicationErrorKnownException {
    // do nothing
  }

}
