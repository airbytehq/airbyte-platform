/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh
import com.google.cloud.storage.StorageOptions
import java.io.IOException
import java.time.Duration
import java.time.OffsetDateTime
import java.util.Date
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private val REFRESH_MARGIN: Duration = Duration.ofMinutes(5)

/** A direct-GCS upload scope and its temporary OAuth token. */
data class GcsLogUploadTarget(
  val accessToken: String,
  val expiresAt: OffsetDateTime,
  val bucketName: String,
  val objectKeyPrefix: String,
) {
  fun isValid(): Boolean =
    accessToken.isNotBlank() &&
      bucketName.isNotBlank() &&
      objectKeyPrefix.startsWith("${DocumentType.LOGS.prefix}/") &&
      objectKeyPrefix.endsWith('/')

  override fun toString(): String = "GcsLogUploadTarget(accessToken=******, expiresAt=$expiresAt, bucketName=******, objectKeyPrefix=******)"
}

/** Provider-neutral refresh outcomes understood by the shared uploader. */
sealed interface LogUploadAuthorizationRefreshResult {
  data class Refreshed(
    val target: GcsLogUploadTarget,
  ) : LogUploadAuthorizationRefreshResult

  data object RetryableFailure : LogUploadAuthorizationRefreshResult

  data object TerminalFailure : LogUploadAuthorizationRefreshResult
}

/**
 * Buffers encoded events and uploads them directly to one immutable GCS object scope.
 * Authorization refreshes update the one credentials object retained by the one GCS client.
 */
class RefreshableGcsLogUploader<T : Any> internal constructor(
  initialTarget: GcsLogUploadTarget,
  private val refreshAuthorization: () -> LogUploadAuthorizationRefreshResult,
  private val encode: (List<T>) -> String,
  private val period: Long = 60L,
  private val unit: TimeUnit = TimeUnit.SECONDS,
  flushSize: Int = BUFFERED_LOG_EVENT_LIMIT,
  private val onFailure: (String) -> Unit = {},
  private val executeTask: (Runnable) -> Unit = CloudStorageBulkUploaderExecutor::executeTask,
  storageClientFactory: (String, OAuth2CredentialsWithRefresh) -> StorageClient,
) {
  private enum class State {
    ACTIVE,
    STOPPING,
    DISABLED,
  }

  private val initialTarget = initialTarget.also { require(it.isValid()) { "GCS log upload authorization is invalid." } }
  private val flushSize = flushSize.also { require(it > 0) { "Flush size must be positive." } }
  private val lifecycleLock = ReentrantLock()
  private val uploadFinished = lifecycleLock.newCondition()
  private val buffer = ArrayDeque<T>()
  private var state = State.ACTIVE
  private var uploadInFlight = false
  private var uploadThread: Thread? = null
  private var thresholdUploadScheduled = false
  private var uploadTask: ScheduledFuture<*>? = null

  private val credentials: OAuth2CredentialsWithRefresh =
    OAuth2CredentialsWithRefresh
      .newBuilder()
      .apply {
        setAccessToken(initialTarget.toAccessToken())
        setRefreshMargin(REFRESH_MARGIN)
        setExpirationMargin(REFRESH_MARGIN)
        setRefreshHandler { refreshAccessToken() }
      }.build()

  private val storageClient: StorageClient = storageClientFactory(initialTarget.bucketName, credentials)

  constructor(
    initialTarget: GcsLogUploadTarget,
    refreshAuthorization: () -> LogUploadAuthorizationRefreshResult,
    encode: (List<T>) -> String,
    period: Long = 60L,
    unit: TimeUnit = TimeUnit.SECONDS,
    flushSize: Int = BUFFERED_LOG_EVENT_LIMIT,
    onFailure: (String) -> Unit = {},
  ) : this(
    initialTarget = initialTarget,
    refreshAuthorization = refreshAuthorization,
    encode = encode,
    period = period,
    unit = unit,
    flushSize = flushSize,
    onFailure = onFailure,
    storageClientFactory = ::buildDirectGcsStorageClient,
  )

  val isDisabled: Boolean
    get() = lifecycleLock.withLock { state == State.DISABLED }

  fun start() {
    try {
      lifecycleLock.withLock {
        if (state == State.ACTIVE && uploadTask == null) {
          uploadTask = CloudStorageBulkUploaderExecutor.scheduleTask(this::flush, period, period, unit)
        }
      }
    } catch (_: Throwable) {
      reportFailure(SCHEDULE_FAILURE_MESSAGE)
    }
  }

  fun append(event: T) {
    val shouldScheduleUpload =
      lifecycleLock.withLock {
        if (state != State.ACTIVE) return
        buffer.addLast(event)
        reserveThresholdUploadLocked()
      }
    if (shouldScheduleUpload) {
      scheduleThresholdUpload()
    }
  }

  fun flush() {
    val pendingUpload = lifecycleLock.withLock { reserveActiveUploadLocked() } ?: return
    upload(pendingUpload)
  }

  fun stop() {
    var periodicTask: ScheduledFuture<*>? = null
    val action =
      lifecycleLock.withLock {
        when (state) {
          State.ACTIVE -> {
            state = State.STOPPING
            thresholdUploadScheduled = false
            periodicTask = uploadTask
            uploadTask = null
            if (uploadThread === Thread.currentThread()) StopAction.SCHEDULE_DRAIN else StopAction.DRAIN
          }
          State.STOPPING ->
            if (uploadThread === Thread.currentThread()) StopAction.RETURN else StopAction.WAIT
          State.DISABLED -> StopAction.RETURN
        }
      }
    periodicTask?.cancel(false)

    when (action) {
      StopAction.DRAIN -> drainAndDisable()
      StopAction.SCHEDULE_DRAIN -> scheduleShutdownDrain()
      StopAction.WAIT -> waitUntilDisabled()
      StopAction.RETURN -> Unit
    }
  }

  fun disable() {
    lifecycleLock.withLock { disableLocked() }
  }

  private fun refreshAccessToken(): AccessToken {
    if (!tryBeginRemoteCall()) {
      throw IOException(REFRESH_UNAVAILABLE_MESSAGE)
    }
    val result =
      try {
        refreshAuthorization()
      } catch (failure: Throwable) {
        if (failure is VirtualMachineError) throw failure
        if (failure is InterruptedException) Thread.currentThread().interrupt()
        LogUploadAuthorizationRefreshResult.TerminalFailure
      }
    val validatedResult =
      if (result is LogUploadAuthorizationRefreshResult.Refreshed && (!result.target.isValid() || !result.target.hasSameScope(initialTarget))) {
        LogUploadAuthorizationRefreshResult.TerminalFailure
      } else {
        result
      }
    return when (validatedResult) {
      is LogUploadAuthorizationRefreshResult.Refreshed -> validatedResult.target.toAccessToken()
      LogUploadAuthorizationRefreshResult.RetryableFailure -> throw IOException(REFRESH_RETRYABLE_MESSAGE)
      LogUploadAuthorizationRefreshResult.TerminalFailure -> {
        disable()
        throw IOException(REFRESH_UNAVAILABLE_MESSAGE)
      }
    }
  }

  private fun tryBeginRemoteCall(): Boolean = lifecycleLock.withLock { state != State.DISABLED }

  private fun reserveThresholdUploadLocked(): Boolean {
    if (state != State.ACTIVE || buffer.size < flushSize || uploadInFlight || thresholdUploadScheduled) return false
    thresholdUploadScheduled = true
    return true
  }

  private fun scheduleThresholdUpload() {
    try {
      executeTask(Runnable(this::runThresholdUpload))
    } catch (failure: Throwable) {
      if (failure is VirtualMachineError) throw failure
      lifecycleLock.withLock { thresholdUploadScheduled = false }
      reportFailure(SCHEDULE_FAILURE_MESSAGE)
    }
  }

  private fun runThresholdUpload() {
    val pendingUpload =
      lifecycleLock.withLock {
        thresholdUploadScheduled = false
        reserveActiveUploadLocked()
      } ?: return
    upload(pendingUpload)
  }

  private fun reserveActiveUploadLocked(): PendingUpload<T>? {
    if (state != State.ACTIVE || buffer.isEmpty() || uploadInFlight) return null
    return takeBatchLocked()
  }

  private fun takeBatchLocked(): PendingUpload<T> {
    val storageId = createFileId(initialTarget.objectKeyPrefix)
    uploadInFlight = true
    uploadThread = Thread.currentThread()
    val events = ArrayList<T>(minOf(buffer.size, flushSize))
    repeat(minOf(buffer.size, flushSize)) { events += buffer.removeFirst() }
    return PendingUpload(events, storageId)
  }

  private fun upload(pendingUpload: PendingUpload<T>) {
    var failureMessage: String? = null
    try {
      val document = encode(pendingUpload.events)
      if (!tryBeginRemoteCall()) return
      storageClient.write(pendingUpload.storageId, document)
    } catch (failure: Throwable) {
      if (failure is VirtualMachineError) throw failure
      failureMessage = UPLOAD_FAILURE_MESSAGE
    } finally {
      val shouldScheduleUpload =
        lifecycleLock.withLock {
          uploadInFlight = false
          uploadThread = null
          uploadFinished.signalAll()
          reserveThresholdUploadLocked()
        }
      failureMessage?.let(::reportFailure)
      if (shouldScheduleUpload) scheduleThresholdUpload()
    }
  }

  private fun drainAndDisable() {
    try {
      while (true) {
        val pendingUpload =
          lifecycleLock.withLock {
            while (state == State.STOPPING && uploadInFlight) {
              uploadFinished.await()
            }
            if (state != State.STOPPING) return
            if (buffer.isEmpty()) {
              disableLocked()
              return
            }
            takeBatchLocked()
          }
        upload(pendingUpload)
      }
    } catch (failure: Throwable) {
      if (failure is VirtualMachineError) throw failure
      if (failure is InterruptedException) Thread.currentThread().interrupt()
      reportFailure(SHUTDOWN_FAILURE_MESSAGE)
      disable()
    }
  }

  private fun scheduleShutdownDrain() {
    try {
      executeTask(Runnable(this::drainAndDisable))
    } catch (failure: Throwable) {
      if (failure is VirtualMachineError) throw failure
      reportFailure(SHUTDOWN_FAILURE_MESSAGE)
      disable()
    }
  }

  private fun waitUntilDisabled() {
    try {
      lifecycleLock.withLock {
        while (state == State.STOPPING) {
          uploadFinished.await()
        }
      }
    } catch (failure: InterruptedException) {
      Thread.currentThread().interrupt()
      reportFailure(SHUTDOWN_FAILURE_MESSAGE)
    }
  }

  private fun disableLocked() {
    state = State.DISABLED
    thresholdUploadScheduled = false
    buffer.clear()
    uploadTask?.cancel(false)
    uploadTask = null
    uploadFinished.signalAll()
  }

  private fun reportFailure(message: String) {
    try {
      onFailure(message)
    } catch (_: Throwable) {
      // Logging failures must not affect workload execution.
    }
  }

  private fun GcsLogUploadTarget.hasSameScope(other: GcsLogUploadTarget): Boolean =
    bucketName == other.bucketName && objectKeyPrefix == other.objectKeyPrefix

  private fun GcsLogUploadTarget.toAccessToken(): AccessToken = AccessToken(accessToken, Date.from(expiresAt.toInstant()))

  private data class PendingUpload<T>(
    val events: List<T>,
    val storageId: String,
  )

  private enum class StopAction {
    DRAIN,
    SCHEDULE_DRAIN,
    WAIT,
    RETURN,
  }

  private companion object {
    const val REFRESH_RETRYABLE_MESSAGE = "GCS log upload authorization refresh failed."
    const val REFRESH_UNAVAILABLE_MESSAGE = "GCS log upload authorization is unavailable."
    const val SCHEDULE_FAILURE_MESSAGE = "Unable to schedule GCS log uploads."
    const val SHUTDOWN_FAILURE_MESSAGE = "Unable to finish the final GCS log upload."
    const val UPLOAD_FAILURE_MESSAGE = "Unable to upload the current log batch."
  }
}

private fun buildDirectGcsStorageClient(
  bucketName: String,
  credentials: OAuth2CredentialsWithRefresh,
): StorageClient {
  val gcsClient =
    StorageOptions
      .newBuilder()
      .setCredentials(credentials)
      .build()
      .service
  return GcsStorageClient.fromPrebuiltClient(bucketName, DocumentType.LOGS, gcsClient)
}
