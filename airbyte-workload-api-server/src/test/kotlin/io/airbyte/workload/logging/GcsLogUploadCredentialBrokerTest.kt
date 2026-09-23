/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.logging

import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.ComputeEngineCredentials
import com.google.auth.oauth2.CredentialAccessBoundary
import com.google.auth.oauth2.DownscopedCredentials
import com.google.auth.oauth2.GoogleCredentials
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.unmockkStatic
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Date

class GcsLogUploadCredentialBrokerTest {
  private val sourceCredentials = mockk<GoogleCredentials>()
  private val credentialsProvider = mockk<ComputeEngineCredentialsProvider>()
  private val tokenProvider = mockk<GoogleDownscopedTokenProvider>()

  @Test
  fun `creates metadata credentials without acquiring a token`() {
    val credentials = ComputeEngineCredentialsProvider().get()

    assertInstanceOf(ComputeEngineCredentials::class.java, credentials)
    assertNull(credentials.accessToken)
  }

  @Test
  fun `downscopes the source credentials directly`() {
    val expiration = Instant.parse("2026-09-02T13:00:00Z")
    val accessToken = AccessToken("downscoped-token", Date.from(expiration))
    val boundary = mockk<CredentialAccessBoundary>()
    val builder = mockk<DownscopedCredentials.Builder>()
    val downscopedCredentials = mockk<DownscopedCredentials>()
    mockkStatic(DownscopedCredentials::class)
    try {
      every { DownscopedCredentials.newBuilder() } returns builder
      every { builder.setSourceCredential(any()) } returns builder
      every { builder.setCredentialAccessBoundary(any()) } returns builder
      every { builder.build() } returns downscopedCredentials
      every { downscopedCredentials.refreshAccessToken() } returns accessToken

      val issuedToken = GoogleDownscopedTokenProvider().issue(sourceCredentials, boundary)

      assertEquals(accessToken, issuedToken)
      verify(exactly = 1) { builder.setSourceCredential(sourceCredentials) }
    } finally {
      unmockkStatic(DownscopedCredentials::class)
    }
  }

  @Test
  fun `constructs an exact objectCreator credential access boundary`() {
    val expiration = Instant.parse("2026-09-02T13:00:00Z")
    every { credentialsProvider.get() } returns sourceCredentials
    every { tokenProvider.issue(any(), any()) } answers {
      val boundary = arg<CredentialAccessBoundary>(1)
      val rules = boundary.accessBoundaryRules
      val condition = requireNotNull(rules.single().availabilityCondition)

      assertEquals(1, rules.size)
      assertEquals("//storage.googleapis.com/projects/_/buckets/managed-log-bucket", rules.single().availableResource)
      assertEquals(listOf("inRole:roles/storage.objectCreator"), rules.single().availablePermissions)
      assertEquals(
        "resource.name.startsWith('projects/_/buckets/managed-log-bucket/objects/job-logging/job/7/attempt/2/')",
        condition.expression,
      )
      AccessToken("authoritative-token", Date.from(expiration))
    }
    val broker = broker()

    val authorization = broker.issue("managed-log-bucket", "job-logging/job/7/attempt/2/")

    assertEquals("authoritative-token", authorization.accessToken)
    assertEquals(OffsetDateTime.ofInstant(expiration, ZoneOffset.UTC), authorization.expiresAt)
    assertEquals("managed-log-bucket", authorization.bucketName)
    assertEquals("job-logging/job/7/attempt/2/", authorization.objectKeyPrefix)
    verify(exactly = 1) { tokenProvider.issue(sourceCredentials, any()) }
  }

  @Test
  fun `acquires metadata credentials and exchanges a fresh token on every issuance`() {
    every { credentialsProvider.get() } returns sourceCredentials
    every { tokenProvider.issue(any(), any()) } returnsMany
      listOf(
        AccessToken("first-token", Date.from(Instant.parse("2026-09-02T13:00:00Z"))),
        AccessToken("second-token", Date.from(Instant.parse("2026-09-02T13:01:00Z"))),
      )
    val broker = broker()

    assertEquals("first-token", broker.issue("bucket", "job-logging/one/").accessToken)
    assertEquals("second-token", broker.issue("bucket", "job-logging/one/").accessToken)

    verify(exactly = 2) { credentialsProvider.get() }
    verify(exactly = 2) { tokenProvider.issue(sourceCredentials, any()) }
  }

  @Test
  fun `acquires metadata credentials only while issuing authorization`() {
    val expiration = Instant.parse("2026-09-02T13:00:00Z")
    every { credentialsProvider.get() } returns sourceCredentials
    every { tokenProvider.issue(any(), any()) } returns
      AccessToken("downscoped-token", Date.from(expiration))
    val broker = broker()

    verify(exactly = 0) { credentialsProvider.get() }
    verify(exactly = 0) { tokenProvider.issue(any(), any()) }

    val authorization = broker.issue("bucket", "job-logging/one/")

    assertEquals("downscoped-token", authorization.accessToken)
    verify(exactly = 1) { credentialsProvider.get() }
    verify(exactly = 1) { tokenProvider.issue(sourceCredentials, any()) }
  }

  @Test
  fun `rejects a token without an authoritative expiration`() {
    every { credentialsProvider.get() } returns sourceCredentials
    every { tokenProvider.issue(any(), any()) } returns AccessToken("token", null)

    val error = assertThrows<IllegalStateException> { broker().issue("bucket", "job-logging/one/") }

    assertEquals("GCS returned a token without an expiration.", error.message)
  }

  private fun broker() =
    GcsLogUploadCredentialBroker(
      credentialsProvider,
      tokenProvider,
    )
}
