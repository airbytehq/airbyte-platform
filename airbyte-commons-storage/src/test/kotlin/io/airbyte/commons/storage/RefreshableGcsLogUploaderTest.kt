/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import com.google.api.client.util.Clock
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh
import com.google.auth.oauth2.useTestClock
import io.airbyte.micronaut.runtime.StorageType
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.net.URI
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Collections
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

internal class RefreshableGcsLogUploaderTest {
  @Test
  fun `first uploaded object ID uses batch reservation time`() {
    val writtenIds = mutableListOf<String>()
    val uploader = uploader(TestStorageClient(BUCKET) { id, _ -> writtenIds += id })
    val constructedAt = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS)

    while (!LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).isAfter(constructedAt)) {
      Thread.sleep(10)
    }
    val beforeReservation = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS)
    uploader.append("event")
    uploader.flush()

    val idTimestamp =
      LocalDateTime.parse(
        writtenIds.single().removePrefix(PREFIX).substringBefore('_'),
        DateTimeFormatter.ofPattern("yyyyMMddHHmmss"),
      )
    assertFalse(idTimestamp.isBefore(beforeReservation))
  }

  @Test
  fun `refreshes at five minute boundary twice through one credentials and storage client without changing scope`() {
    val clock = MutableClock(Instant.parse("2030-01-01T00:00:00Z"))
    val initial = target("token-1", "2030-01-01T00:10:00Z")
    val refreshed =
      ArrayDeque(
        listOf(
          target("token-2", "2030-01-01T00:20:00Z"),
          target("token-3", "2030-01-01T00:30:00Z"),
        ),
      )
    val refreshes = AtomicInteger()
    val credentialsSeen = mutableListOf<OAuth2CredentialsWithRefresh>()
    val bucketsSeen = mutableListOf<String>()
    val writes = mutableListOf<String>()
    val storageClient = TestStorageClient(BUCKET) { id, _ -> writes += id }

    val uploader =
      RefreshableGcsLogUploader<String>(
        initialTarget = initial,
        refreshAuthorization = {
          refreshes.incrementAndGet()
          LogUploadAuthorizationRefreshResult.Refreshed(refreshed.removeFirst())
        },
        encode = { it.joinToString(",") },
        storageClientFactory = { bucket, credentials ->
          bucketsSeen += bucket
          credentialsSeen += credentials
          storageClient
        },
      )

    assertEquals(1, credentialsSeen.size)
    assertEquals(listOf(BUCKET), bucketsSeen)
    val credentials = credentialsSeen.single().apply { useTestClock(clock) }
    assertEquals(
      initial.expiresAt.toInstant(),
      credentials
        .accessToken.expirationTime
        .toInstant(),
    )

    clock.advanceTo(Instant.parse("2030-01-01T00:04:59Z"))
    credentials.getRequestMetadata(URI.create("https://storage.test"))
    assertEquals(0, refreshes.get())
    assertEquals("token-1", credentials.accessToken.tokenValue)

    clock.advanceTo(Instant.parse("2030-01-01T00:05:00Z"))
    credentials.getRequestMetadata(URI.create("https://storage.test"))
    assertEquals(1, refreshes.get())
    assertEquals("token-2", credentials.accessToken.tokenValue)
    assertEquals(
      OffsetDateTime.parse("2030-01-01T00:20:00Z").toInstant(),
      credentials
        .accessToken.expirationTime
        .toInstant(),
    )

    clock.advanceTo(Instant.parse("2030-01-01T00:14:59Z"))
    credentials.getRequestMetadata(URI.create("https://storage.test"))
    assertEquals(1, refreshes.get())

    clock.advanceTo(Instant.parse("2030-01-01T00:15:01Z"))
    credentials.getRequestMetadata(URI.create("https://storage.test"))
    assertEquals(2, refreshes.get())
    assertEquals("token-3", credentials.accessToken.tokenValue)
    assertEquals(1, credentialsSeen.size)
    assertEquals(listOf(BUCKET), bucketsSeen)

    runBlocking { uploader.append("event") }
    uploader.flush()
    assertEquals(1, writes.size)
    assertTrue(writes.single().startsWith(PREFIX))
  }

  @Test
  fun `invalid initial target fails before constructing a storage client`() {
    val storageClients = AtomicInteger()

    assertThrows(IllegalArgumentException::class.java) {
      RefreshableGcsLogUploader<String>(
        initialTarget = target("token", "2030-01-01T00:00:00Z").copy(objectKeyPrefix = "invalid"),
        refreshAuthorization = { LogUploadAuthorizationRefreshResult.TerminalFailure },
        encode = { it.single() },
        storageClientFactory = { _, _ ->
          storageClients.incrementAndGet()
          TestStorageClient(BUCKET) { _, _ -> }
        },
      )
    }

    assertEquals(0, storageClients.get())
  }

  @Test
  fun `unexpected uploader failure remains non-fatal and drops the batch`() {
    val uploader = uploader(TestStorageClient(BUCKET) { _, _ -> throw AssertionError("sensitive") })
    runBlocking { uploader.append("event") }

    assertDoesNotThrow { uploader.flush() }
    assertFalse(uploader.isDisabled)
  }

  @Test
  fun `drops failed upload and accepts a later batch`() {
    val attempts = AtomicInteger()
    val storageIds = mutableListOf<String>()
    val uploaded = mutableListOf<String>()
    val storageClient =
      TestStorageClient(BUCKET) { storageId, document ->
        storageIds += storageId
        if (attempts.incrementAndGet() == 1) throw IOException("sensitive upload failure")
        uploaded += document
      }
    val statuses = mutableListOf<String>()
    val uploader = uploader(storageClient = storageClient, onFailure = statuses::add)

    runBlocking { uploader.append("first") }
    assertDoesNotThrow { uploader.flush() }
    runBlocking { uploader.append("second") }
    uploader.flush()

    assertEquals(listOf("second"), uploaded)
    assertNotEquals(storageIds.first(), storageIds.last())
    assertFalse(uploader.isDisabled)
    assertTrue(statuses.all { !it.contains("sensitive upload failure") && !it.contains(PREFIX) })
  }

  @Test
  fun `retryable refresh failure drops only its batch and later refresh succeeds`() {
    lateinit var credentials: OAuth2CredentialsWithRefresh
    val refreshes = AtomicInteger()
    val uploaded = mutableListOf<String>()
    val storageClient =
      TestStorageClient(BUCKET) { _, document ->
        credentials.refresh()
        uploaded += document
      }
    val uploader =
      RefreshableGcsLogUploader<String>(
        initialTarget = target("token-1", "2030-01-01T00:00:00Z"),
        refreshAuthorization = {
          if (refreshes.incrementAndGet() == 1) {
            LogUploadAuthorizationRefreshResult.RetryableFailure
          } else {
            LogUploadAuthorizationRefreshResult.Refreshed(target("token-2", "2030-01-01T00:10:00Z"))
          }
        },
        encode = { it.single() },
        storageClientFactory = { _, createdCredentials ->
          credentials = createdCredentials
          storageClient
        },
      )

    runBlocking { uploader.append("first") }
    uploader.flush()
    runBlocking { uploader.append("second") }
    uploader.flush()

    assertEquals(listOf("second"), uploaded)
    assertEquals(2, refreshes.get())
    assertFalse(uploader.isDisabled)
  }

  @Test
  fun `unexpected refresh throwable disables without exposing diagnostics or starting later work`() {
    lateinit var credentials: OAuth2CredentialsWithRefresh
    val refreshes = AtomicInteger()
    val writes = AtomicInteger()
    val diagnostics = mutableListOf<String>()
    val uploader =
      RefreshableGcsLogUploader<String>(
        initialTarget = target("token-1", "2030-01-01T00:00:00Z"),
        refreshAuthorization = {
          refreshes.incrementAndGet()
          throw AssertionError("sensitive token at $PREFIX")
        },
        encode = { it.single() },
        onFailure = diagnostics::add,
        storageClientFactory = { _, createdCredentials ->
          credentials = createdCredentials
          TestStorageClient(BUCKET) { _, _ ->
            writes.incrementAndGet()
            credentials.refresh()
          }
        },
      )

    runBlocking { uploader.append("current") }
    assertDoesNotThrow { uploader.flush() }

    assertTrue(uploader.isDisabled)
    runBlocking { uploader.append("ignored") }
    uploader.flush()
    assertEquals(1, writes.get())
    assertEquals(1, refreshes.get())
    assertTrue(diagnostics.isNotEmpty())
    assertTrue(diagnostics.none { it.contains("sensitive") || it.contains("token-1") || it.contains(PREFIX) })
  }

  @Test
  fun `scope-changing refresh permanently disables and clears queued events`() {
    lateinit var credentials: OAuth2CredentialsWithRefresh
    val writes = AtomicInteger()
    val uploader =
      RefreshableGcsLogUploader<String>(
        initialTarget = target("token-1", "2030-01-01T00:00:00Z"),
        refreshAuthorization = {
          LogUploadAuthorizationRefreshResult.Refreshed(
            target("token-2", "2030-01-01T00:10:00Z").copy(objectKeyPrefix = "job-logging/other/"),
          )
        },
        encode = { it.single() },
        storageClientFactory = { _, createdCredentials ->
          credentials = createdCredentials
          TestStorageClient(BUCKET) { _, _ -> writes.incrementAndGet() }
        },
      )

    runBlocking { uploader.append("queued") }
    assertThrows(IOException::class.java) { credentials.refresh() }
    runBlocking { uploader.append("ignored") }
    uploader.flush()

    assertTrue(uploader.isDisabled)
    assertEquals(0, writes.get())
  }

  @Test
  fun `enqueue and disable do not wait for encoding or upload and disabled prevents new remote calls`() {
    val encodeStarted = CountDownLatch(1)
    val releaseEncode = CountDownLatch(1)
    val writeStarted = CountDownLatch(1)
    val releaseWrite = CountDownLatch(1)
    val writes = AtomicInteger()
    val refreshes = AtomicInteger()
    lateinit var credentials: OAuth2CredentialsWithRefresh
    val uploader =
      RefreshableGcsLogUploader<String>(
        initialTarget = target("token-1", "2030-01-01T00:00:00Z"),
        refreshAuthorization = {
          refreshes.incrementAndGet()
          LogUploadAuthorizationRefreshResult.Refreshed(target("token-2", "2030-01-01T00:10:00Z"))
        },
        encode = {
          encodeStarted.countDown()
          releaseEncode.await()
          it.single()
        },
        storageClientFactory = { _, createdCredentials ->
          credentials = createdCredentials
          TestStorageClient(BUCKET) { _, _ ->
            writes.incrementAndGet()
            writeStarted.countDown()
            releaseWrite.await()
          }
        },
      )
    runBlocking { uploader.append("in-flight") }
    val executor = Executors.newFixedThreadPool(3)

    val upload = executor.submit { uploader.flush() }
    try {
      assertTrue(encodeStarted.await(5, TimeUnit.SECONDS))
      executor.submit { runBlocking { uploader.append("queued-during-encoding") } }.get(5, TimeUnit.SECONDS)

      releaseEncode.countDown()
      assertTrue(writeStarted.await(5, TimeUnit.SECONDS))
      executor.submit { runBlocking { uploader.append("queued-during-upload") } }.get(5, TimeUnit.SECONDS)

      executor.submit { uploader.disable() }.get(5, TimeUnit.SECONDS)
      assertTrue(uploader.isDisabled)
      runBlocking { uploader.append("ignored") }
      uploader.flush()
      assertThrows(IOException::class.java) { credentials.refresh() }
    } finally {
      releaseEncode.countDown()
      releaseWrite.countDown()
      upload.get(5, TimeUnit.SECONDS)
      executor.shutdownNow()
    }

    assertEquals(1, writes.get())
    assertEquals(0, refreshes.get())
  }

  @Test
  fun `terminal refresh during shutdown is non-fatal and disables future work`() {
    lateinit var credentials: OAuth2CredentialsWithRefresh
    val writes = AtomicInteger()
    val uploader =
      RefreshableGcsLogUploader<String>(
        initialTarget = target("token-1", "2030-01-01T00:00:00Z"),
        refreshAuthorization = { LogUploadAuthorizationRefreshResult.TerminalFailure },
        encode = { it.single() },
        storageClientFactory = { _, createdCredentials ->
          credentials = createdCredentials
          TestStorageClient(BUCKET) { _, _ ->
            credentials.refresh()
            writes.incrementAndGet()
          }
        },
      )
    uploader.start()
    runBlocking { uploader.append("final") }

    assertDoesNotThrow { uploader.stop() }
    runBlocking { uploader.append("ignored") }
    uploader.flush()

    assertTrue(uploader.isDisabled)
    assertEquals(0, writes.get())
  }

  @Test
  fun `concurrent stop callers wait for an active upload and drain every accepted event exactly once`() {
    val writeStarted = CountDownLatch(1)
    val releaseWrite = CountDownLatch(1)
    val writeAttempts = AtomicInteger()
    val storageIds = mutableListOf<String>()
    val documents = mutableListOf<String>()
    val uploader =
      RefreshableGcsLogUploader<String>(
        initialTarget = target("token-1", "2030-01-01T00:00:00Z"),
        refreshAuthorization = { LogUploadAuthorizationRefreshResult.TerminalFailure },
        encode = { it.joinToString(",") },
        flushSize = 2,
        storageClientFactory = { _, _ ->
          TestStorageClient(BUCKET) { storageId, document ->
            synchronized(documents) {
              storageIds += storageId
              documents += document
            }
            if (writeAttempts.incrementAndGet() == 1) {
              writeStarted.countDown()
              releaseWrite.await()
            }
          }
        },
      )
    val stopCallerCount = 3
    val executor = Executors.newFixedThreadPool(stopCallerCount + 1)
    runBlocking { uploader.append("event-1") }
    val activeUpload = executor.submit { uploader.flush() }

    try {
      assertTrue(writeStarted.await(5, TimeUnit.SECONDS))
      runBlocking {
        uploader.append("event-2")
        uploader.append("event-3")
        uploader.append("event-4")
      }
      val stopCallersReady = CountDownLatch(stopCallerCount)
      val startStops = CountDownLatch(1)
      val stops =
        (1..stopCallerCount).map {
          executor.submit {
            stopCallersReady.countDown()
            startStops.await()
            uploader.stop()
          }
        }
      assertTrue(stopCallersReady.await(5, TimeUnit.SECONDS))
      startStops.countDown()

      stops.forEach { stop -> assertThrows(TimeoutException::class.java) { stop.get(1, TimeUnit.SECONDS) } }
      runBlocking { uploader.append("ignored-during-stop") }
      releaseWrite.countDown()
      stops.forEach { stop -> assertDoesNotThrow { stop.get(5, TimeUnit.SECONDS) } }
      activeUpload.get(5, TimeUnit.SECONDS)

      val uploadedEvents = synchronized(documents) { documents.flatMap { it.split(',') } }
      assertEquals(listOf("event-1", "event-2", "event-3", "event-4"), uploadedEvents)
      assertTrue(synchronized(documents) { documents.all { it.split(',').size <= 2 } })
      assertEquals(storageIds.size, storageIds.toSet().size)
      assertTrue(uploader.isDisabled)

      val writesAfterStop = writeAttempts.get()
      assertDoesNotThrow { executor.submit { uploader.stop() }.get(5, TimeUnit.SECONDS) }
      runBlocking { uploader.append("ignored-after-stop") }
      uploader.flush()
      assertEquals(writesAfterStop, writeAttempts.get())
    } finally {
      releaseWrite.countDown()
      executor.shutdownNow()
    }
  }

  @Test
  fun `stop from encode callback returns and drains accepted events before disabling`() {
    val callbackCompleted = CountDownLatch(1)
    val uploadedBatches = Collections.synchronizedList(mutableListOf<Pair<String, String>>())
    val refreshes = AtomicInteger()
    val encodes = AtomicInteger()
    val drainExecutor = CapturingTaskExecutor()
    val uploadExecutor = Executors.newSingleThreadExecutor()
    val uploaderReference = AtomicReference<RefreshableGcsLogUploader<String>>()
    val uploader =
      RefreshableGcsLogUploader<String>(
        initialTarget = target("token-1", "2030-01-01T00:00:00Z"),
        refreshAuthorization = {
          refreshes.incrementAndGet()
          LogUploadAuthorizationRefreshResult.TerminalFailure
        },
        encode = { events ->
          if (encodes.incrementAndGet() == 1) {
            val callbackUploader = uploaderReference.get()
            callbackUploader.append("encode-queued-1")
            callbackUploader.append("encode-queued-2")
            callbackUploader.stop()
            callbackUploader.stop()
            callbackCompleted.countDown()
          }
          events.joinToString(",")
        },
        flushSize = 3,
        executeTask = drainExecutor::execute,
        storageClientFactory = { _, _ ->
          TestStorageClient(BUCKET) { storageId, document -> uploadedBatches += storageId to document }
        },
      )
    uploaderReference.set(uploader)
    uploader.append("encode-active")

    try {
      val upload = uploadExecutor.submit { uploader.flush() }
      assertTrue(callbackCompleted.await(5, TimeUnit.SECONDS))
      assertDoesNotThrow { upload.get(5, TimeUnit.SECONDS) }
      assertDoesNotThrow { drainExecutor.awaitCompletion() }
    } finally {
      uploadExecutor.shutdownNow()
      drainExecutor.close()
    }

    val completedBatches = synchronized(uploadedBatches) { uploadedBatches.toList() }
    assertEquals(listOf("encode-active", "encode-queued-1", "encode-queued-2"), completedBatches.flatMap { it.second.split(',') })
    assertTrue(completedBatches.all { it.second.split(',').size <= 3 })
    assertEquals(completedBatches.size, completedBatches.map { it.first }.toSet().size)
    assertEquals(0, refreshes.get())
    assertTrue(uploader.isDisabled)

    val writesAfterStop = completedBatches.size
    uploader.append("ignored-after-stop")
    uploader.flush()
    assertEquals(writesAfterStop, synchronized(uploadedBatches) { uploadedBatches.size })
    assertEquals(0, refreshes.get())
  }

  @Test
  fun `stop from authorization refresh callback returns and drains accepted events before disabling`() {
    val callbackCompleted = CountDownLatch(1)
    val uploadedBatches = Collections.synchronizedList(mutableListOf<Pair<String, String>>())
    val refreshes = AtomicInteger()
    val writeCallbacks = AtomicInteger()
    val drainExecutor = CapturingTaskExecutor()
    val uploadExecutor = Executors.newSingleThreadExecutor()
    lateinit var credentials: OAuth2CredentialsWithRefresh
    val uploaderReference = AtomicReference<RefreshableGcsLogUploader<String>>()
    val uploader =
      RefreshableGcsLogUploader<String>(
        initialTarget = target("token-1", "2030-01-01T00:00:00Z"),
        refreshAuthorization = {
          refreshes.incrementAndGet()
          val callbackUploader = uploaderReference.get()
          callbackUploader.append("refresh-queued-1")
          callbackUploader.append("refresh-queued-2")
          callbackUploader.stop()
          callbackUploader.stop()
          callbackCompleted.countDown()
          LogUploadAuthorizationRefreshResult.Refreshed(target("token-2", "2030-01-01T00:10:00Z"))
        },
        encode = { it.joinToString(",") },
        flushSize = 3,
        executeTask = drainExecutor::execute,
        storageClientFactory = { _, createdCredentials ->
          credentials = createdCredentials
          TestStorageClient(BUCKET) { storageId, document ->
            if (writeCallbacks.incrementAndGet() == 1) credentials.refresh()
            uploadedBatches += storageId to document
          }
        },
      )
    uploaderReference.set(uploader)
    uploader.append("refresh-active")

    try {
      val upload = uploadExecutor.submit { uploader.flush() }
      assertTrue(callbackCompleted.await(5, TimeUnit.SECONDS))
      assertDoesNotThrow { upload.get(5, TimeUnit.SECONDS) }
      assertDoesNotThrow { drainExecutor.awaitCompletion() }
    } finally {
      uploadExecutor.shutdownNow()
      drainExecutor.close()
    }

    val completedBatches = synchronized(uploadedBatches) { uploadedBatches.toList() }
    assertEquals(listOf("refresh-active", "refresh-queued-1", "refresh-queued-2"), completedBatches.flatMap { it.second.split(',') })
    assertTrue(completedBatches.all { it.second.split(',').size <= 3 })
    assertEquals(completedBatches.size, completedBatches.map { it.first }.toSet().size)
    assertEquals(1, refreshes.get())
    assertTrue(uploader.isDisabled)

    val writesAfterStop = writeCallbacks.get()
    uploader.append("ignored-after-stop")
    uploader.flush()
    assertEquals(writesAfterStop, writeCallbacks.get())
    assertEquals(1, refreshes.get())
  }

  @Test
  fun `stop from storage write callback returns and drains accepted events before disabling`() {
    val callbackCompleted = CountDownLatch(1)
    val uploadedBatches = Collections.synchronizedList(mutableListOf<Pair<String, String>>())
    val refreshes = AtomicInteger()
    val writeCallbacks = AtomicInteger()
    val drainExecutor = CapturingTaskExecutor()
    val uploadExecutor = Executors.newSingleThreadExecutor()
    val uploaderReference = AtomicReference<RefreshableGcsLogUploader<String>>()
    val uploader =
      RefreshableGcsLogUploader<String>(
        initialTarget = target("token-1", "2030-01-01T00:00:00Z"),
        refreshAuthorization = {
          refreshes.incrementAndGet()
          LogUploadAuthorizationRefreshResult.TerminalFailure
        },
        encode = { it.joinToString(",") },
        flushSize = 3,
        executeTask = drainExecutor::execute,
        storageClientFactory = { _, _ ->
          TestStorageClient(BUCKET) { storageId, document ->
            uploadedBatches += storageId to document
            if (writeCallbacks.incrementAndGet() == 1) {
              val callbackUploader = uploaderReference.get()
              callbackUploader.append("write-queued-1")
              callbackUploader.append("write-queued-2")
              callbackUploader.stop()
              callbackUploader.stop()
              callbackCompleted.countDown()
            }
          }
        },
      )
    uploaderReference.set(uploader)
    uploader.append("write-active")

    try {
      val upload = uploadExecutor.submit { uploader.flush() }
      assertTrue(callbackCompleted.await(5, TimeUnit.SECONDS))
      assertDoesNotThrow { upload.get(5, TimeUnit.SECONDS) }
      assertDoesNotThrow { drainExecutor.awaitCompletion() }
    } finally {
      uploadExecutor.shutdownNow()
      drainExecutor.close()
    }

    val completedBatches = synchronized(uploadedBatches) { uploadedBatches.toList() }
    assertEquals(listOf("write-active", "write-queued-1", "write-queued-2"), completedBatches.flatMap { it.second.split(',') })
    assertTrue(completedBatches.all { it.second.split(',').size <= 3 })
    assertEquals(completedBatches.size, completedBatches.map { it.first }.toSet().size)
    assertEquals(0, refreshes.get())
    assertTrue(uploader.isDisabled)

    val writesAfterStop = writeCallbacks.get()
    uploader.append("ignored-after-stop")
    uploader.flush()
    assertEquals(writesAfterStop, writeCallbacks.get())
    assertEquals(0, refreshes.get())
  }

  @Test
  fun `rejected shutdown drain disables without leaking or starting later work`() {
    val callbackCompleted = CountDownLatch(1)
    val encodedBatches = mutableListOf<List<String>>()
    val writes = AtomicInteger()
    val refreshes = AtomicInteger()
    val submissions = AtomicInteger()
    val diagnostics = mutableListOf<String>()
    val uploaderReference = AtomicReference<RefreshableGcsLogUploader<String>>()
    val uploader =
      RefreshableGcsLogUploader<String>(
        initialTarget = target("sensitive-token", "2030-01-01T00:00:00Z"),
        refreshAuthorization = {
          refreshes.incrementAndGet()
          LogUploadAuthorizationRefreshResult.TerminalFailure
        },
        encode = { events: List<String> ->
          encodedBatches += events
          val callbackUploader = uploaderReference.get()
          callbackUploader.append("queued-before-terminal-disable")
          callbackUploader.stop()
          callbackCompleted.countDown()
          events.joinToString(",")
        },
        flushSize = 3,
        onFailure = { message: String -> diagnostics.add(message) },
        executeTask = { _: Runnable ->
          submissions.incrementAndGet()
          throw RejectedExecutionException("sensitive-token $BUCKET $PREFIX/private/path")
        },
        storageClientFactory = { _: String, _: OAuth2CredentialsWithRefresh ->
          TestStorageClient(BUCKET) { _, _ -> writes.incrementAndGet() }
        },
      )
    uploaderReference.set(uploader)
    uploader.append("accepted-upload")
    val executor = Executors.newSingleThreadExecutor()

    try {
      assertDoesNotThrow { executor.submit { uploader.flush() }.get(5, TimeUnit.SECONDS) }
      assertTrue(callbackCompleted.await(5, TimeUnit.SECONDS))
    } finally {
      executor.shutdownNow()
    }

    assertEquals(listOf(listOf("accepted-upload")), encodedBatches)
    assertEquals(0, writes.get())
    assertEquals(0, refreshes.get())
    assertEquals(1, submissions.get())
    assertEquals(listOf("Unable to finish the final GCS log upload."), diagnostics)
    assertTrue(diagnostics.none { it.contains("sensitive") || it.contains(BUCKET) || it.contains(PREFIX) || it.contains("private/path") })
    assertTrue(uploader.isDisabled)

    uploader.append("ignored-after-disable")
    uploader.flush()
    uploader.stop()
    assertEquals(0, writes.get())
    assertEquals(0, refreshes.get())
    assertEquals(1, submissions.get())
  }

  private fun uploader(
    storageClient: StorageClient,
    onFailure: (String) -> Unit = {},
  ): RefreshableGcsLogUploader<String> =
    RefreshableGcsLogUploader(
      initialTarget = target("token-1", "2030-01-01T00:00:00Z"),
      refreshAuthorization = { LogUploadAuthorizationRefreshResult.TerminalFailure },
      encode = { it.single() },
      onFailure = onFailure,
      storageClientFactory = { _, _ -> storageClient },
    )

  private fun target(
    token: String,
    expiresAt: String,
  ) = GcsLogUploadTarget(
    accessToken = token,
    expiresAt = OffsetDateTime.parse(expiresAt).withOffsetSameInstant(ZoneOffset.UTC),
    bucketName = BUCKET,
    objectKeyPrefix = PREFIX,
  )

  private companion object {
    const val BUCKET = "bucket"
    const val PREFIX = "job-logging/workload/"
  }
}

private class MutableClock(
  instant: Instant,
) : Clock {
  private var currentTimeMillis = instant.toEpochMilli()

  override fun currentTimeMillis(): Long = currentTimeMillis

  fun advanceTo(instant: Instant) {
    currentTimeMillis = instant.toEpochMilli()
  }
}

private class CapturingTaskExecutor : AutoCloseable {
  private val executor = Executors.newSingleThreadExecutor()
  private val submittedTask = CompletableFuture<java.util.concurrent.Future<*>>()

  fun execute(task: Runnable) {
    check(submittedTask.complete(executor.submit(task))) { "Expected one deferred task." }
  }

  fun awaitCompletion() {
    submittedTask.get(5, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS)
  }

  override fun close() {
    executor.shutdownNow()
  }
}

private class TestStorageClient(
  override val bucketName: String,
  private val writer: (String, String) -> Unit,
) : StorageClient {
  override val documentType = DocumentType.LOGS
  override val storageType = StorageType.GCS

  override fun write(
    id: String,
    document: String,
  ) = writer(id, document)

  override fun list(id: String): List<String> = error("not used")

  override fun read(id: String): String? = error("not used")

  override fun delete(id: String): Boolean = error("not used")
}
