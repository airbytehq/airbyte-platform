/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.nimbusds.jwt.JWTParser
import io.airbyte.data.repositories.AbstractConfigRepositoryTest
import io.airbyte.data.services.ServiceAccountsService
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.UUID

@MicronautTest
class ServiceAccountsServiceTest : AbstractConfigRepositoryTest() {
  var clock = Clock.fixed(Instant.now(), ZoneId.of("UTC"))
  lateinit var svc: ServiceAccountsService

  @BeforeEach
  fun setup() {
    svc = context.getBean(ServiceAccountsService::class.java)
    svc.clock
  }

  @Test
  fun basic() {
    val a = svc.create("test")
    assertEquals("test", a.name)

    val b = svc.get(a.id)
    assertEquals("test", b!!.name)

    svc.delete(b.id)
    assertEquals(null, svc.get(b.id))

    assertEquals(null, svc.get(UUID.randomUUID()))

    val c = svc.create("test-managed", managed = true)
    val d = svc.getManagedByName("test-managed")
    assertEquals(c.id, d!!.id)
  }

  @Test
  fun token() {
    val a = svc.create("test")
    val tok = svc.generateToken(a.id, a.secret)
    val parsed = JWTParser.parse(tok)
    assertEquals(a.id.toString(), parsed.jwtClaimsSet.subject)
    assertEquals(ServiceAccountsService.SERVICE_ACCOUNT_TOKEN_TYPE, parsed.jwtClaimsSet.getClaimAsString("typ"))
    assertEquals(clock.instant().plus(15, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS), parsed.jwtClaimsSet.expirationTime.toInstant())
  }
}
