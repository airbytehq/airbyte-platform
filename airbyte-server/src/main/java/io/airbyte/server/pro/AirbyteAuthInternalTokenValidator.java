/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.pro;

import static io.airbyte.config.persistence.UserPersistence.DEFAULT_USER_ID;

import io.airbyte.commons.auth.AirbyteAuthConstants;
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled;
import io.airbyte.commons.server.support.RbacRoleHelper;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.token.validator.TokenValidator;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Token validator for internal Airbyte auth. This is used to authenticate internal requests between
 * Airbyte services. Traffic between internal services is assumed to be trusted, so this is not a
 * means of security, but rather a mechanism for identifying and granting roles to the service that
 * is making the internal request. The webapp proxy unsets the X-Airbyte-Auth header, so this header
 * will only be present on internal requests.
 **/
@Slf4j
@Singleton
@RequiresAirbyteProEnabled
public class AirbyteAuthInternalTokenValidator implements TokenValidator<HttpRequest<?>> {

  @Override
  public Publisher<Authentication> validateToken(final String token, final HttpRequest<?> request) {
    if (validateAirbyteAuthInternalToken(token)) {
      return Flux.create(emitter -> {
        emitter.next(getAuthentication());
        emitter.complete();
      });
    } else {
      // pass to the next validator, if one exists
      return Flux.empty();
    }
  }

  private Boolean validateAirbyteAuthInternalToken(final String token) {
    return AirbyteAuthConstants.VALID_INTERNAL_SERVICE_NAMES.contains(token);
  }

  private Authentication getAuthentication() {
    // set the Authentication username to the token value, which must be a valid internal service name.
    // for now, all internal services get instance admin roles.
    return Authentication.build(DEFAULT_USER_ID.toString(), RbacRoleHelper.getInstanceAdminRoles());
  }

}
