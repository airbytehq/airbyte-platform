/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import com.github.benmanes.caffeine.cache.Ticker
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.EnableStrictJsonDeserialization
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.server.apis.controllers.ConnectionApiController
import io.airbyte.server.apis.controllers.DataplaneGroupApiController
import io.airbyte.server.apis.controllers.OrganizationApiController
import io.airbyte.server.apis.controllers.WebBackendApiController
import io.airbyte.server.apis.controllers.WorkspaceApiController
import io.micronaut.context.annotation.Replaces
import io.micronaut.core.annotation.Order
import io.micronaut.core.io.buffer.ByteBuffer
import io.micronaut.core.type.Argument
import io.micronaut.core.type.Headers
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Consumes
import io.micronaut.http.body.MessageBodyReader
import io.micronaut.http.context.ServerHttpRequestContext
import io.micronaut.jackson.databind.JacksonDatabindMapper
import io.micronaut.json.JsonMapper
import io.micronaut.json.body.JsonMessageHandler
import io.micronaut.web.router.RouteAttributes
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.InputStream
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicReference

@Singleton
@Order(JsonMessageHandler.ORDER - 1)
@Consumes(
  MediaType.APPLICATION_JSON,
  "text/json",
  "application/hal+json",
  "application/vnd.github+json",
  "application/feed+json",
  "application/problem+json",
  "application/json-patch+json",
  "application/merge-patch+json",
  "application/schema+json",
)
class StrictApiJsonMessageBodyReader<T>(
  jsonMapper: JsonMapper,
  objectMapper: ObjectMapper,
  private val strictJsonDeserializationFlag: StrictJsonDeserializationFlag,
) : MessageBodyReader<T> {
  private val lenientReader = JsonMessageHandler<T>(jsonMapper)
  private val strictReader =
    JsonMessageHandler<T>(
      JacksonDatabindMapper(
        objectMapper.copy().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES),
      ),
    )

  override fun isReadable(
    type: Argument<T>,
    mediaType: MediaType,
  ): Boolean = lenientReader.isReadable(type, mediaType)

  override fun read(
    type: Argument<T>,
    mediaType: MediaType,
    httpHeaders: Headers,
    byteBuffer: ByteBuffer<*>,
  ): T = reader().read(type, mediaType, httpHeaders, byteBuffer)

  override fun read(
    type: Argument<T>,
    mediaType: MediaType,
    httpHeaders: Headers,
    inputStream: InputStream,
  ): T = reader().read(type, mediaType, httpHeaders, inputStream)

  private fun reader(): JsonMessageHandler<T> =
    if (
      !isScimRequest() &&
      isAllowlistedApiControllerRequest() &&
      strictJsonDeserializationFlag.isEnabled()
    ) {
      strictReader
    } else {
      lenientReader
    }

  private fun isScimRequest(): Boolean =
    ServerHttpRequestContext
      .find<Any>()
      .map { request -> request.path == SCIM_BASE_PATH || request.path.startsWith("$SCIM_BASE_PATH/") }
      .orElse(false)

  private fun isAllowlistedApiControllerRequest(): Boolean =
    ServerHttpRequestContext
      .find<Any>()
      .flatMap(RouteAttributes::getRouteInfo)
      .map { routeInfo ->
        generateSequence<Class<*>>(routeInfo.declaringType) { it.superclass }
          .any {
            it in STRICT_API_CONTROLLERS ||
              it
                .getAnnotation(Replaces::class.java)
                ?.value
                ?.java
                ?.let(STRICT_API_CONTROLLERS::contains) == true
          }
      }.orElse(false)

  private companion object {
    const val SCIM_BASE_PATH = "/scim/v2"
    val STRICT_API_CONTROLLERS =
      setOf(
        OrganizationApiController::class.java,
        WorkspaceApiController::class.java,
        ConnectionApiController::class.java,
        WebBackendApiController::class.java,
        DataplaneGroupApiController::class.java,
      )
  }
}

@Singleton
class StrictJsonDeserializationFlag {
  private val cachedValue: LoadingCache<Unit, CachedValue>
  private val retrySubmissionAfter = AtomicReference<Long?>()

  @Inject
  constructor(
    featureFlagClient: FeatureFlagClient,
    @Named(AirbyteTaskExecutors.IO) executor: Executor,
  ) : this(featureFlagClient, executor, Ticker.systemTicker())

  internal constructor(
    featureFlagClient: FeatureFlagClient,
    refreshExecutor: Executor,
    ticker: Ticker,
  ) {
    cachedValue =
      Caffeine
        .newBuilder()
        .executor(DIRECT_EXECUTOR)
        .ticker(ticker)
        .refreshAfterWrite(REFRESH_INTERVAL)
        .build<Unit, CachedValue>(
          object : CacheLoader<Unit, CachedValue> {
            override fun load(key: Unit): CachedValue = evaluate()

            override fun reload(
              key: Unit,
              oldValue: CachedValue,
            ): CachedValue =
              try {
                evaluate()
              } catch (e: Exception) {
                if (e is InterruptedException) {
                  Thread.currentThread().interrupt()
                }
                CachedValue(oldValue.enabled)
              }

            override fun asyncReload(
              key: Unit,
              oldValue: CachedValue,
              ignoredExecutor: Executor,
            ): CompletableFuture<out CachedValue> {
              val retryAfter = retrySubmissionAfter.get()
              if (retryAfter != null && ticker.read() - retryAfter <= 0) {
                return CompletableFuture.completedFuture(CachedValue(oldValue.enabled))
              }

              return try {
                CompletableFuture.supplyAsync({ reload(key, oldValue) }, refreshExecutor)
              } catch (_: RejectedExecutionException) {
                retrySubmissionAfter.set(ticker.read() + REFRESH_INTERVAL.toNanos())
                CompletableFuture.completedFuture(CachedValue(oldValue.enabled))
              }
            }

            private fun evaluate(): CachedValue = CachedValue(featureFlagClient.boolVariation(EnableStrictJsonDeserialization, Empty))
          },
        ).apply {
          put(Unit, CachedValue(EnableStrictJsonDeserialization.default))
        }
  }

  fun isEnabled(): Boolean = cachedValue.get(Unit).enabled

  private data class CachedValue(
    val enabled: Boolean,
  )

  private companion object {
    val DIRECT_EXECUTOR: Executor = Executor(Runnable::run)
    val REFRESH_INTERVAL: Duration = Duration.ofSeconds(30)
  }
}
