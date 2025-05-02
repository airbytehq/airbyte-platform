/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.pro

import io.airbyte.commons.auth.AirbyteAuthConstants
import io.airbyte.commons.auth.AuthRole
import io.airbyte.config.persistence.UserPersistence
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.validator.TokenValidator
import jakarta.inject.Singleton
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink

/**
 * Token validator for internal Airbyte auth. This is used to authenticate internal requests between
 * Airbyte services. Traffic between internal services is assumed to be trusted, so this is not a
 * means of security, but rather a mechanism for identifying and granting roles to the service that
 * is making the internal request. The webapp proxy unsets the X-Airbyte-Auth header, so this header
 * will only be present on internal requests.
 */
@Singleton
@Requires(property = "micronaut.security.enabled", value = "true")
@Requires(property = "airbyte.edition", pattern = "(?i)^community|enterprise$")
class AirbyteAuthInternalTokenValidator : TokenValidator<HttpRequest<*>?> {
  override fun validateToken(
    token: String,
    request: HttpRequest<*>?,
  ): Publisher<Authentication> =
    if (validateAirbyteAuthInternalToken(token)) {
      Flux.create { emitter: FluxSink<Authentication> ->
        emitter.next(authentication)
        emitter.complete()
      }
    } else {
      // pass to the next validator, if one exists
      Flux.empty()
    }

  private fun validateAirbyteAuthInternalToken(token: String): Boolean = AirbyteAuthConstants.VALID_INTERNAL_SERVICE_NAMES.contains(token)

  private val authentication: Authentication
    get() = // set the Authentication username to the token value, which must be a valid internal service name.
      // for now, all internal services get instance admin roles.
      Authentication.build(
        UserPersistence.DEFAULT_USER_ID.toString(),
        AuthRole.getInstanceAdminRoles(),
      )
}
