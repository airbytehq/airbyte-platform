/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.pro;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_MEMBER;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;
import static org.mockito.Mockito.mock;

import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
import java.util.Collection;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

class AirbyteAuthInternalTokenValidatorTest {

  private final AirbyteAuthInternalTokenValidator airbyteAuthInternalTokenValidator = new AirbyteAuthInternalTokenValidator();

  @Test
  void testValidateTokenIsWorker() {
    final String token = "worker";
    final HttpRequest httpRequest = mock(HttpRequest.class);

    final Set<String> expectedRoles = Set.of(
        ADMIN,
        WORKSPACE_ADMIN,
        WORKSPACE_EDITOR,
        WORKSPACE_READER,
        ORGANIZATION_ADMIN,
        ORGANIZATION_EDITOR,
        ORGANIZATION_READER,
        ORGANIZATION_MEMBER,
        AUTHENTICATED_USER);

    final Publisher<Authentication> responsePublisher = airbyteAuthInternalTokenValidator.validateToken(token, httpRequest);

    StepVerifier.create(responsePublisher)
        .expectNextMatches(r -> matchSuccessfulResponse(r, expectedRoles))
        .verifyComplete();
  }

  @Test
  void testValidateTokenIsNotWorker() {
    final String token = "not-worker";
    final HttpRequest httpRequest = mock(HttpRequest.class);

    final Publisher<Authentication> responsePublisher = airbyteAuthInternalTokenValidator.validateToken(token, httpRequest);

    // Verify the stream remains empty.
    StepVerifier.create(responsePublisher)
        .expectComplete()
        .verify();
  }

  private boolean matchSuccessfulResponse(final Authentication authentication,
                                          final Collection<String> expectedRoles) {
    return authentication.getRoles().containsAll(expectedRoles);
  }

}
