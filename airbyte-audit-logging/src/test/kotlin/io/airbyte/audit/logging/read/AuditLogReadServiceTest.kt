/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.read

import io.airbyte.api.problems.model.generated.ProblemLicenseEntitlementData
import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.audit.logging.model.Actor
import io.airbyte.audit.logging.model.AuditLogEntry
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.AuditLoggingEntitlement
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.storage.StorageClient
import io.airbyte.domain.models.OrganizationId
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

class AuditLogReadServiceTest {
  private lateinit var storageClient: StorageClient
  private lateinit var entitlementService: EntitlementService
  private lateinit var service: AuditLogReadService

  @BeforeEach
  fun setUp() {
    storageClient = mockk()
    entitlementService = entitlementService(entitled = true)
    service = AuditLogReadService(BASE_STORAGE_ID, storageClient, MAX_FILES_PER_REQUEST, entitlementService)
  }

  @Test
  fun `returns entries sorted by timestamp descending across days and files`() {
    val dayOne = LocalDate.parse("2026-08-14")
    val dayTwo = LocalDate.parse("2026-08-13")
    val fileOne = fileId(dayOne, "20260814100000")
    val fileTwo = fileId(dayTwo, "20260813100000")
    val newest = entry(at("2026-08-14T10:00:00Z"))
    val middle = entry(at("2026-08-14T09:00:00Z"))
    val oldest = entry(at("2026-08-13T10:00:00Z"))

    stubFiles(mapOf(dayOne to listOf(fileOne), dayTwo to listOf(fileTwo)))
    stubReads(mapOf(fileOne to Jsons.serialize(listOf(middle, newest)), fileTwo to Jsons.serialize(listOf(oldest))))

    val page = service.listAuditLogs(query())

    assertEquals(listOf(newest, middle, oldest).map { it.id }, page.entries.map { it.id })
    assertNull(page.nextPageToken)
  }

  @Test
  fun `only ever lists the query organization prefix`() {
    stubFiles(emptyMap())

    service.listAuditLogs(query())

    verify { storageClient.list(match { it.startsWith("$BASE_STORAGE_ID/$ORGANIZATION_ID/") }) }
  }

  @Test
  fun `workspace filter matches only entries in that workspace`() {
    val day = LocalDate.parse("2026-08-14")
    val file = fileId(day, "20260814100000")
    val matching = entry(at("2026-08-14T10:00:00Z"), workspaceId = WORKSPACE_ID)
    val otherWorkspace = entry(at("2026-08-14T09:00:00Z"), workspaceId = UUID.randomUUID())
    val noWorkspace = entry(at("2026-08-14T08:00:00Z"), workspaceId = null)

    stubFiles(mapOf(day to listOf(file)))
    stubReads(mapOf(file to Jsons.serialize(listOf(matching, otherWorkspace, noWorkspace))))

    val page = service.listAuditLogs(query(workspaceId = WORKSPACE_ID))

    assertEquals(listOf(matching.id), page.entries.map { it.id })
  }

  @Test
  fun `no workspace filter returns entries with and without a workspace`() {
    val day = LocalDate.parse("2026-08-14")
    val file = fileId(day, "20260814100000")
    val withWorkspace = entry(at("2026-08-14T10:00:00Z"), workspaceId = WORKSPACE_ID)
    val noWorkspace = entry(at("2026-08-14T08:00:00Z"), workspaceId = null)

    stubFiles(mapOf(day to listOf(file)))
    stubReads(mapOf(file to Jsons.serialize(listOf(withWorkspace, noWorkspace))))

    val page = service.listAuditLogs(query())

    assertEquals(setOf(withWorkspace.id, noWorkspace.id), page.entries.map { it.id }.toSet())
  }

  @Test
  fun `actor filter matches only the given actor`() {
    val day = LocalDate.parse("2026-08-14")
    val file = fileId(day, "20260814100000")
    val matching = entry(at("2026-08-14T10:00:00Z"), actorId = "user-1")
    val otherActor = entry(at("2026-08-14T09:00:00Z"), actorId = "user-2")
    val noActor = entry(at("2026-08-14T08:00:00Z")).copy(actor = null)

    stubFiles(mapOf(day to listOf(file)))
    stubReads(mapOf(file to Jsons.serialize(listOf(matching, otherActor, noActor))))

    val page = service.listAuditLogs(query(actorId = "user-1"))

    assertEquals(listOf(matching.id), page.entries.map { it.id })
  }

  @Test
  fun `operation filter matches only the given operation`() {
    val day = LocalDate.parse("2026-08-14")
    val file = fileId(day, "20260814100000")
    val matching = entry(at("2026-08-14T10:00:00Z"), operation = "updateSsoConfig")
    val other = entry(at("2026-08-14T09:00:00Z"), operation = "createConnection")

    stubFiles(mapOf(day to listOf(file)))
    stubReads(mapOf(file to Jsons.serialize(listOf(matching, other))))

    val page = service.listAuditLogs(query(operation = "updateSsoConfig"))

    assertEquals(listOf(matching.id), page.entries.map { it.id })
  }

  @Test
  fun `success filter matches only failures when false`() {
    val day = LocalDate.parse("2026-08-14")
    val file = fileId(day, "20260814100000")
    val failure = entry(at("2026-08-14T10:00:00Z"), success = false, errorMessage = "boom")
    val success = entry(at("2026-08-14T09:00:00Z"), success = true)

    stubFiles(mapOf(day to listOf(file)))
    stubReads(mapOf(file to Jsons.serialize(listOf(failure, success))))

    val page = service.listAuditLogs(query(success = false))

    assertEquals(listOf(failure.id), page.entries.map { it.id })
  }

  @Test
  fun `search text matches case-insensitively against operation actor and error message`() {
    val day = LocalDate.parse("2026-08-14")
    val file = fileId(day, "20260814100000")
    val byOperation = entry(at("2026-08-14T10:00:00Z"), operation = "updateSsoConfig")
    val byError = entry(at("2026-08-14T09:00:00Z"), success = false, errorMessage = "SSO provider unreachable")
    val byEmail = entry(at("2026-08-14T08:00:00Z")).copy(actor = Actor(actorId = "user-9", email = "sso-admin@airbyte.io"))
    val noMatch = entry(at("2026-08-14T07:00:00Z"), operation = "createConnection")

    stubFiles(mapOf(day to listOf(file)))
    stubReads(mapOf(file to Jsons.serialize(listOf(byOperation, byError, byEmail, noMatch))))

    val page = service.listAuditLogs(query(searchText = "sso"))

    assertEquals(setOf(byOperation.id, byError.id, byEmail.id), page.entries.map { it.id }.toSet())
  }

  @Test
  fun `entries outside the exact time range are excluded`() {
    val day = LocalDate.parse("2026-08-14")
    val file = fileId(day, "20260814100000")
    val inRange = entry(at("2026-08-14T10:00:00Z"))
    val tooEarly = entry(at("2026-08-13T23:00:00Z"))
    val tooLate = entry(at("2026-08-14T23:00:00Z"))

    stubFiles(mapOf(day to listOf(file)))
    stubReads(mapOf(file to Jsons.serialize(listOf(inRange, tooEarly, tooLate))))

    val page =
      service.listAuditLogs(
        query(startTime = at("2026-08-14T00:00:00Z"), endTime = at("2026-08-14T12:00:00Z")),
      )

    assertEquals(listOf(inRange.id), page.entries.map { it.id })
  }

  @Test
  fun `paginates across files with a resume token`() {
    val day = LocalDate.parse("2026-08-14")
    val fileOne = fileId(day, "20260814120000")
    val fileTwo = fileId(day, "20260814110000")
    val first = entry(at("2026-08-14T12:00:00Z"))
    val second = entry(at("2026-08-14T11:30:00Z"))
    val third = entry(at("2026-08-14T11:00:00Z"))

    stubFiles(mapOf(day to listOf(fileOne, fileTwo)))
    stubReads(mapOf(fileOne to Jsons.serialize(listOf(first, second)), fileTwo to Jsons.serialize(listOf(third))))

    val pageOne = service.listAuditLogs(query(pageSize = 2))
    assertEquals(listOf(first.id, second.id), pageOne.entries.map { it.id })
    assertTrue(pageOne.nextPageToken != null)

    val pageTwo = service.listAuditLogs(query(pageSize = 2, pageToken = pageOne.nextPageToken))
    assertEquals(listOf(third.id), pageTwo.entries.map { it.id })
    assertNull(pageTwo.nextPageToken)
  }

  @Test
  fun `scan cap stops early with a resume token that continues the scan`() {
    val cappedService = AuditLogReadService(BASE_STORAGE_ID, storageClient, maxFilesPerRequest = 1, entitlementService)
    val day = LocalDate.parse("2026-08-14")
    val fileOne = fileId(day, "20260814120000")
    val fileTwo = fileId(day, "20260814110000")
    val first = entry(at("2026-08-14T12:00:00Z"))
    val second = entry(at("2026-08-14T11:00:00Z"))

    stubFiles(mapOf(day to listOf(fileOne, fileTwo)))
    stubReads(mapOf(fileOne to Jsons.serialize(listOf(first)), fileTwo to Jsons.serialize(listOf(second))))

    val pageOne = cappedService.listAuditLogs(query())
    assertEquals(listOf(first.id), pageOne.entries.map { it.id })
    assertTrue(pageOne.nextPageToken != null)

    val pageTwo = cappedService.listAuditLogs(query(pageToken = pageOne.nextPageToken))
    assertEquals(listOf(second.id), pageTwo.entries.map { it.id })
    assertNull(pageTwo.nextPageToken)
  }

  @Test
  fun `normalizes full storage keys before reading`() {
    val day = LocalDate.parse("2026-08-14")
    val relativeId = fileId(day, "20260814100000")
    val cloudStyleKey = "audit-logging/$relativeId"
    val found = entry(at("2026-08-14T10:00:00Z"))

    every { storageClient.list(any()) } answers {
      val id = firstArg<String>()
      if (id.endsWith("/$day")) listOf(cloudStyleKey) else emptyList()
    }
    stubReads(mapOf(relativeId to Jsons.serialize(listOf(found))))

    val page = service.listAuditLogs(query())

    assertEquals(listOf(found.id), page.entries.map { it.id })
    verify { storageClient.read(relativeId) }
  }

  @Test
  fun `empty organization returns an empty page without a token`() {
    stubFiles(emptyMap())

    val page = service.listAuditLogs(query())

    assertTrue(page.entries.isEmpty())
    assertNull(page.nextPageToken)
  }

  @Test
  fun `unreadable and unparseable blobs are skipped`() {
    val day = LocalDate.parse("2026-08-14")
    val corrupt = fileId(day, "20260814120000")
    val throwing = fileId(day, "20260814110000")
    val missing = fileId(day, "20260814105000")
    val healthy = fileId(day, "20260814100000")
    val found = entry(at("2026-08-14T10:00:00Z"))

    stubFiles(mapOf(day to listOf(corrupt, throwing, missing, healthy)))
    every { storageClient.read(corrupt) } returns "{ not json"
    every { storageClient.read(throwing) } throws RuntimeException("storage down")
    every { storageClient.read(missing) } returns null
    every { storageClient.read(healthy) } returns Jsons.serialize(listOf(found))

    val page = service.listAuditLogs(query())

    assertEquals(listOf(found.id), page.entries.map { it.id })
  }

  @Test
  fun `malformed page token is rejected`() {
    assertThrows<IllegalArgumentException> {
      service.listAuditLogs(query(pageToken = "not-a-valid-token!!!"))
    }
  }

  @Test
  fun `page token outside the query range is rejected`() {
    val token =
      AuditLogPageTokenCodec.encode(
        AuditLogPageToken(date = LocalDate.parse("2026-07-01"), fileId = "some-file", index = 0),
      )

    assertThrows<IllegalArgumentException> {
      service.listAuditLogs(query(pageToken = token))
    }
  }

  @Test
  fun `page token referencing an unknown file is rejected`() {
    val day = LocalDate.parse("2026-08-14")
    stubFiles(mapOf(day to listOf(fileId(day, "20260814100000"))))
    val token =
      AuditLogPageTokenCodec.encode(
        AuditLogPageToken(date = day, fileId = fileId(day, "99999999999999"), index = 0),
      )

    assertThrows<IllegalArgumentException> {
      service.listAuditLogs(query(pageToken = token))
    }
  }

  @Test
  fun `query validation rejects invalid ranges and page sizes`() {
    assertThrows<IllegalArgumentException> {
      query(startTime = at("2026-08-14T00:00:00Z"), endTime = at("2026-08-13T00:00:00Z"))
    }
    assertThrows<IllegalArgumentException> {
      query(startTime = at("2026-05-01T00:00:00Z"), endTime = at("2026-08-14T00:00:00Z"))
    }
    assertThrows<IllegalArgumentException> { query(pageSize = 0) }
    assertThrows<IllegalArgumentException> { query(pageSize = AuditLogQuery.MAX_PAGE_SIZE + 1) }
  }

  @Test
  fun `query defaults apply a 30 day lookback and default page size`() {
    val built = AuditLogQuery.of(organizationId = ORGANIZATION_ID)

    assertEquals(
      AuditLogQuery.DEFAULT_LOOKBACK_DAYS,
      java.time.Duration
        .between(built.startTime, built.endTime)
        .toDays(),
    )
    assertEquals(AuditLogQuery.DEFAULT_PAGE_SIZE, built.pageSize)
  }

  @Test
  fun `page token codec round-trips and rejects malformed values`() {
    val token = AuditLogPageToken(date = LocalDate.parse("2026-08-14"), fileId = "base/x/2026-08-14/f.json", index = 7)
    assertEquals(token, AuditLogPageTokenCodec.decode(AuditLogPageTokenCodec.encode(token)))

    assertThrows<IllegalArgumentException> { AuditLogPageTokenCodec.decode("%%%") }
    assertThrows<IllegalArgumentException> {
      AuditLogPageTokenCodec.decode(
        java.util.Base64
          .getUrlEncoder()
          .encodeToString("2026-08-14||3".toByteArray()),
      )
    }
    assertThrows<IllegalArgumentException> {
      AuditLogPageTokenCodec.decode(
        java.util.Base64
          .getUrlEncoder()
          .encodeToString("2026-08-14|file|-1".toByteArray()),
      )
    }
  }

  @Test
  fun `an organization without the audit logging entitlement cannot list logs`() {
    val denying = entitlementService(entitled = false)
    val deniedService = AuditLogReadService(BASE_STORAGE_ID, storageClient, MAX_FILES_PER_REQUEST, denying)

    assertThrows<LicenseEntitlementProblem> { deniedService.listAuditLogs(query()) }

    verify { denying.ensureEntitled(OrganizationId(ORGANIZATION_ID), AuditLoggingEntitlement) }
    // the entitlement gate runs before any storage access
    verify(exactly = 0) { storageClient.list(any()) }
  }

  @Test
  fun `the entitlement is checked against the query organization`() {
    val organizationId = UUID.randomUUID()
    stubFiles(emptyMap())

    service.listAuditLogs(query(organizationId = organizationId))

    verify { entitlementService.ensureEntitled(OrganizationId(organizationId), AuditLoggingEntitlement) }
  }

  private fun entitlementService(entitled: Boolean): EntitlementService =
    mockk {
      if (entitled) {
        every { ensureEntitled(any(), AuditLoggingEntitlement) } returns Unit
      } else {
        every { ensureEntitled(any(), AuditLoggingEntitlement) } throws
          LicenseEntitlementProblem(ProblemLicenseEntitlementData().entitlement(AuditLoggingEntitlement.featureId))
      }
    }

  private fun query(
    organizationId: UUID = ORGANIZATION_ID,
    startTime: Instant = at("2026-08-13T00:00:00Z"),
    endTime: Instant = at("2026-08-14T23:59:59Z"),
    workspaceId: UUID? = null,
    actorId: String? = null,
    operation: String? = null,
    success: Boolean? = null,
    searchText: String? = null,
    pageSize: Int = AuditLogQuery.DEFAULT_PAGE_SIZE,
    pageToken: String? = null,
  ): AuditLogQuery =
    AuditLogQuery(
      organizationId = organizationId,
      startTime = startTime,
      endTime = endTime,
      workspaceId = workspaceId,
      actorId = actorId,
      operation = operation,
      success = success,
      searchText = searchText,
      pageSize = pageSize,
      pageToken = pageToken,
    )

  private fun entry(
    timestamp: Instant,
    organizationId: UUID = ORGANIZATION_ID,
    workspaceId: UUID? = WORKSPACE_ID,
    actorId: String = "user-1",
    operation: String = "updateConnection",
    success: Boolean = true,
    errorMessage: String? = null,
  ): AuditLogEntry =
    AuditLogEntry(
      id = UUID.randomUUID(),
      timestamp = timestamp.toEpochMilli(),
      actor = Actor(actorId = actorId, email = "$actorId@airbyte.io"),
      operation = operation,
      success = success,
      errorMessage = errorMessage,
      organizationId = organizationId,
      workspaceId = workspaceId,
    )

  private fun at(value: String): Instant = Instant.parse(value)

  private fun fileId(
    date: LocalDate,
    timestampPart: String,
  ): String = "$BASE_STORAGE_ID/$ORGANIZATION_ID/$date/${timestampPart}_host_abcdef123456.json"

  private fun stubFiles(filesByDate: Map<LocalDate, List<String>>) {
    every { storageClient.list(any()) } answers {
      val id = firstArg<String>()
      filesByDate[LocalDate.parse(id.substringAfterLast("/"))] ?: emptyList()
    }
  }

  private fun stubReads(documents: Map<String, String>) {
    every { storageClient.read(any()) } answers { documents[firstArg()] }
  }

  companion object {
    private const val BASE_STORAGE_ID = "audit-base"
    private const val MAX_FILES_PER_REQUEST = 100
    private val ORGANIZATION_ID: UUID = UUID.randomUUID()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
  }
}
