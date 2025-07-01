/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.nimbusds.jwt.JWTParser
import io.airbyte.data.auth.AirbyteJwtGeneratorNoAuthImpl
import io.airbyte.data.auth.TokenType
import io.airbyte.data.repositories.AbstractConfigRepositoryTest
import io.airbyte.data.services.ServiceAccountsService
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.UUID

@MicronautTest
class ServiceAccountsServiceTest : AbstractConfigRepositoryTest() {
  var clock = Clock.fixed(Instant.ofEpochMilli(1749850369), ZoneId.of("UTC"))
  lateinit var svc: ServiceAccountsService

  @BeforeEach
  fun setup() {
    svc = context.getBean(ServiceAccountsService::class.java)
    val gen = context.getBean(AirbyteJwtGeneratorNoAuthImpl::class.java)
    gen.clock = clock
  }

  @Test
  fun basic() {
    val a = svc.create(name = "test")
    assertEquals("test", a.name)

    val b = svc.get(a.id)
    assertEquals("test", b!!.name)

    svc.delete(b.id)
    assertEquals(null, svc.get(b.id))

    assertEquals(null, svc.get(UUID.randomUUID()))

    val c = svc.create(name = "test-managed", managed = true)
    val d = svc.getManagedByName("test-managed")
    assertEquals(c.id, d!!.id)
  }

  @Test
  fun token() {
    val a = svc.create(name = "test")
    val tok = svc.generateToken(a.id, a.secret)
    val parsed = JWTParser.parse(tok)

    assertEquals(a.id.toString(), parsed.jwtClaimsSet.subject)
    assertEquals(TokenType.SERVICE_ACCOUNT.toString(), parsed.jwtClaimsSet.getClaimAsString("typ"))
    assertEquals(clock.instant().plus(15, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS), parsed.jwtClaimsSet.expirationTime.toInstant())
  }

  @Test
  fun getAndVerifyTest() {
    val account = svc.create(name = "test")

    val retrieved = svc.getAndVerify(account.id, account.secret)
    assertNotNull(retrieved)
    assertEquals(account.id, retrieved.id)
    // the account secret should not equal the retrieved secret, since the db secret is encoded
    assert(account.secret != retrieved.secret)
  }
}
