/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.analytics

import com.segment.analytics.Analytics
import com.segment.analytics.Callback
import com.segment.analytics.Plugin
import com.segment.analytics.messages.IdentifyMessage
import com.segment.analytics.messages.Message
import com.segment.analytics.messages.TrackMessage
import io.airbyte.api.client.model.generated.DeploymentMetadataRead
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.config.Organization
import io.airbyte.config.ScopeType
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.cache.annotation.CacheConfig
import io.micronaut.cache.annotation.Cacheable
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.http.context.ServerRequestContext
import jakarta.annotation.PreDestroy
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.lang.Thread.sleep
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiFunction
import java.util.function.Function
import java.util.function.Supplier

const val SEGMENT_WRITE_KEY_ENV_VAR = "SEGMENT_WRITE_KEY"
const val TRACKING_STRATEGY_ENV_VAR = "TRACKING_STRATEGY"

private val logger = KotlinLogging.logger {}

/**
 * General interface for user level Airbyte usage reporting. We use Segment for behavioural
 * reporting, so this interface mirrors the Segment backend api sdk.
 *
 *
 * For more information see
 * https://segment.com/docs/connections/sources/catalog/libraries/server/http-api/.
 *
 *
 * This interface allows us to easily stub this out via the [LoggingTrackingClient]. The main
 * implementation is in [SegmentTrackingClient].
 *
 *
 * Although the methods seem to take in workspace id, this id is used to index into more metadata.
 * See [SegmentTrackingClient] for more information.
 *
 *
 * Keep in mind that this interface is also relied on in Airbyte Cloud.
 */
interface TrackingClient {
  fun identify(
    scopeId: UUID,
    scopeType: ScopeType,
  )

  fun track(
    scopeId: UUID,
    scopeType: ScopeType,
    action: String?,
  )

  fun track(
    scopeId: UUID,
    scopeType: ScopeType,
    action: String?,
    metadata: Map<String, Any?>,
  )
}

const val AIRBYTE_ROLE = "airbyte_role"
const val INSTALLATION_ID = "installation_id"
const val UNKNOWN = "unknown"

/**
 * This class is a wrapper around the Segment backend Java SDK.
 * <p>
 * In general, the Segment SDK events have two pieces to them, a top-level userId field and a map of
 * properties.
 * <p>
 * As of 2021/11/03, the top level userId field is standardised on the
 * [io.airbyte.config.StandardWorkspace.getCustomerId] field. This field is a random UUID generated when a
 * workspace model is created. This standardisation is through OSS Airbyte and Cloud Airbyte. This
 * join key now underpins Airbyte OSS Segment tracking. Although the id is meaningless and the name
 * confusing, it is not worth performing a migration at this time. Interested parties can look at
 * https://github.com/airbytehq/airbyte/issues/7456 for more context.
 * <p>
 * Consumers utilising this class must understand that the top-level userId field is subject to this
 * constraint.
 * <p>
 * See the following document for details on tracked events. Please update this document if tracked
 * events change.
 * https://docs.google.com/spreadsheets/d/1lGLmLIhiSPt_-oaEf3CpK-IxXnCO0NRHurvmWldoA2w/edit#gid=1567609168
 */
@Singleton
@Requires(property = "airbyte.tracking.strategy", pattern = "(?i)^segment$")
@Named("trackingClient")
class SegmentTrackingClient(
  private val segmentAnalyticsClient: SegmentAnalyticsClient,
  private val trackingIdentityFetcher: TrackingIdentityFetcher,
  private val deploymentFetcher: DeploymentFetcher,
  @Value("\${airbyte.role}") val airbyteRole: String,
  @Value("\${airbyte.installation-id}") val installationId: UUID? = null,
) : TrackingClient {
  override fun identify(
    scopeId: UUID,
    scopeType: ScopeType,
  ) {
    val deployment: Deployment = deploymentFetcher.get()
    val trackingIdentity: TrackingIdentity = trackingIdentityFetcher.apply(scopeId, scopeType)
    val identityMetadata: Map<String, Any?> =
      buildMap {
        // deployment
        put(AIRBYTE_VERSION_KEY, deployment.getDeploymentVersion())
        put("deployment_mode", deployment.getDeploymentMode())
        put("deployment_id", deployment.getDeploymentId().toString())

        // workspace (includes info that in the future we would store in an organization)
        put("anonymized", trackingIdentity.isAnonymousDataCollection())
        put("subscribed_newsletter", trackingIdentity.isNews())
        put("subscribed_security", trackingIdentity.isSecurityUpdates())
        trackingIdentity.email?.let { put("email", it) }

        // other
        airbyteRole.takeIf { it.isNotBlank() }?.let { put(AIRBYTE_ROLE, it) }
        installationId?.let { put(INSTALLATION_ID, it) }
      }

    val joinKey: String = trackingIdentity.customerId.toString()
    segmentAnalyticsClient.analyticsClient.enqueue(
      IdentifyMessage
        .builder() // user id is scoped by workspace. there is no cross-workspace tracking.
        .userId(joinKey)
        .traits(identityMetadata),
    )
  }

  override fun track(
    scopeId: UUID,
    scopeType: ScopeType,
    action: String?,
  ) {
    track(scopeId, scopeType, action, emptyMap())
  }

  override fun track(
    scopeId: UUID,
    scopeType: ScopeType,
    action: String?,
    metadata: Map<String, Any?>,
  ) {
    val deployment: Deployment = deploymentFetcher.get()
    val trackingIdentity: TrackingIdentity = trackingIdentityFetcher.apply(scopeId, scopeType)

    val mapCopy: Map<String, Any?> =
      buildMap {
        putAll(metadata)
        put(AIRBYTE_SOURCE, getAirbyteSource() ?: UNKNOWN)

        // Always add these traits.
        put(AIRBYTE_VERSION_KEY, deployment.getDeploymentVersion())
        put(CUSTOMER_ID_KEY, trackingIdentity.customerId)
        put(AIRBYTE_DEPLOYMENT_ID, deployment.getDeploymentId().toString())
        put(AIRBYTE_DEPLOYMENT_MODE, deployment.getDeploymentMode())
        put(AIRBYTE_TRACKED_AT, Instant.now().toString())
        if (metadata.isNotEmpty() && trackingIdentity.email != null) {
          put("email", trackingIdentity.email)
        }
        installationId?.let { put(INSTALLATION_ID, it) }
      }

    val joinKey: String = trackingIdentity.customerId.toString()
    segmentAnalyticsClient.analyticsClient.enqueue(
      TrackMessage
        .builder(action)
        .userId(joinKey)
        .properties(mapCopy),
    )
  }

  private fun getAirbyteSource(): String? {
    val currentRequest = ServerRequestContext.currentRequest<Any>()
    return if (currentRequest.isPresent) {
      currentRequest.get().headers[AIRBYTE_ANALYTIC_SOURCE_HEADER]
    } else {
      null
    }
  }

  companion object {
    internal const val AIRBYTE_ANALYTIC_SOURCE_HEADER = "X-Airbyte-Analytic-Source"
    internal const val AIRBYTE_DEPLOYMENT_ID = "deployment_id"
    internal const val AIRBYTE_DEPLOYMENT_MODE = "deployment_mode"
    internal const val AIRBYTE_ROLE = "airbyte_role"
    internal const val AIRBYTE_SOURCE = "airbyte_source"
    internal const val AIRBYTE_TRACKED_AT = "tracked_at"
    internal const val AIRBYTE_VERSION_KEY = "airbyte_version"
    internal const val CUSTOMER_ID_KEY = "user_id"
    internal const val INSTALLATION_ID = "installation_id"
    internal const val UNKNOWN = "unknown"
  }
}

@Singleton
@Requires(property = "airbyte.tracking.strategy", pattern = "(?i)^segment$")
class SegmentAnalyticsClient(
  @Value("\${airbyte.tracking.flush-interval-sec:10}") flushInterval: Long,
  @Value("\${airbyte.tracking.write-key}") writeKey: String,
  private val blockingShutdownAnalyticsPlugin: BlockingShutdownAnalyticsPlugin,
) {
  val analyticsClient: Analytics =
    Analytics
      .builder(writeKey)
      .flushInterval(flushInterval, TimeUnit.SECONDS)
      .plugin(blockingShutdownAnalyticsPlugin)
      .build()

  @PreDestroy
  fun close() {
    logger.info { "Closing Segment analytics client..." }
    analyticsClient.flush()
    blockingShutdownAnalyticsPlugin.waitForFlush()
    analyticsClient.shutdown()
    logger.info { "Segment analytics client closed.  No new events will be accepted." }
  }
}

/**
 * Custom Segment Analytic client [Plugin] that ensures that any enqueued message
 * has been sent to Segment.  This can be used on shutdown to verify that we are not
 * dropping any enqueued or in-flight events are delivered to Segment before
 * stopping.
 */
@Singleton
@Requires(property = "airbyte.tracking.strategy", pattern = "(?i)^segment$")
class BlockingShutdownAnalyticsPlugin(
  @Value("\${airbyte.tracking.flush-interval-sec:10}") private val flushInterval: Long,
) : Plugin {
  private val inflightMessageCount = AtomicLong(0L)

  override fun configure(builder: Analytics.Builder) {
    builder.messageTransformer {
      inflightMessageCount.incrementAndGet()
      true
    }

    builder.callback(
      object : Callback {
        override fun success(message: Message) {
          inflightMessageCount.decrementAndGet()
        }

        override fun failure(
          message: Message,
          throwable: Throwable,
        ) {
          logger.error(throwable) {
            "Failed to send analytics message to Segment (userId = ${message.userId()}, type = ${message.type()}, messageId = ${message.messageId()})"
          }
          inflightMessageCount.decrementAndGet()
        }
      },
    )
  }

  fun currentInflightMessageCount(): Long = inflightMessageCount.get()

  fun waitForFlush() {
    // Wait 2 x the flush interval for the flush to occur before moving along to avoid
    // blocking indefinitely on shutdown if something goes wrong
    val timeout = flushInterval * 2

    try {
      logger.info { "Waiting for Segment analytic client to flush enqueued messages..." }
      val future =
        CompletableFuture.supplyAsync {
          var completed = false
          while (!completed) {
            completed = inflightMessageCount.get() == 0L
            if (!completed) {
              sleep(1)
            }
          }
        }
      future.get(timeout, TimeUnit.SECONDS)
      logger.info { "Segment analytic client flush complete." }
    } catch (e: TimeoutException) {
      logger.warn { "Timed out waiting for Segment analytic client to flush enqueued messages (timeout = $timeout seconds)" }
      logger.warn { "There are ${inflightMessageCount.get()} remaining enqueued analytic message(s) that were not sent." }
    }
  }
}

/**
 * Tracking client that logs to STDOUT. Mainly used for local development.
 */
@Singleton
@Requires(property = "airbyte.tracking.strategy", pattern = "(?i)^logging$")
@Named("trackingClient")
class LoggingTrackingClient(
  private val deploymentFetcher: DeploymentFetcher,
  private val trackingIdentityFetcher: TrackingIdentityFetcher,
) : TrackingClient {
  override fun identify(
    scopeId: UUID,
    scopeType: ScopeType,
  ) {
    logger.info { "identify. userId: ${trackingIdentityFetcher.apply(scopeId, scopeType).customerId}" }
  }

  override fun track(
    scopeId: UUID,
    scopeType: ScopeType,
    action: String?,
  ) {
    track(scopeId, scopeType, action, emptyMap())
  }

  override fun track(
    scopeId: UUID,
    scopeType: ScopeType,
    action: String?,
    metadata: Map<String, Any?>,
  ) {
    val deployment: Deployment = deploymentFetcher.get()
    val trackingIdentity: TrackingIdentity = trackingIdentityFetcher.apply(scopeId, scopeType)
    val version: String = deployment.getDeploymentVersion()
    val userId: UUID = trackingIdentity.customerId
    logger.info { "track. version: $version, userId: $userId, action: $action, metadata: $metadata" }
  }
}

@Singleton
@CacheConfig("analytics-tracking-deployments")
open class DeploymentFetcher(
  @Named("deploymentSupplier") val deploymentFetcher: Supplier<DeploymentMetadataRead>,
) : Supplier<Deployment> {
  @Cacheable
  override fun get(): Deployment {
    val deploymentMetadata = deploymentFetcher.get()
    return Deployment(deploymentMetadata)
  }
}

@Singleton
@CacheConfig("analytics-tracking-identity")
open class TrackingIdentityFetcher(
  @Named("workspaceFetcher") val workspaceFetcher: Function<UUID, WorkspaceRead>,
  @Named("organizationFetcher") val organizationFetcher: Function<UUID, Organization>,
) : BiFunction<UUID, ScopeType, TrackingIdentity> {
  @Cacheable
  override fun apply(
    scopeId: UUID,
    scopeType: ScopeType,
  ): TrackingIdentity {
    when (scopeType) {
      ScopeType.WORKSPACE -> {
        val workspaceRead = workspaceFetcher.apply(scopeId)
        val email: String? = workspaceRead.anonymousDataCollection.takeIf { it == false }?.let { workspaceRead.email }

        return TrackingIdentity(
          workspaceRead.customerId,
          email,
          workspaceRead.anonymousDataCollection,
          workspaceRead.news,
          workspaceRead.securityUpdates,
        )
      }
      ScopeType.ORGANIZATION -> {
        val organization = organizationFetcher.apply(scopeId)
        return TrackingIdentity(
          organization.organizationId,
          organization.email,
          anonymousDataCollection = false,
          news = false,
          securityUpdates = false,
        )
      }
    }
  }
}

class Deployment(
  private val deploymentMetadata: DeploymentMetadataRead,
) {
  fun getDeploymentMode(): String = deploymentMetadata.mode

  fun getDeploymentId(): UUID = deploymentMetadata.id

  fun getDeploymentVersion(): String = deploymentMetadata.version
}

data class TrackingIdentity(
  val customerId: UUID,
  val email: String?,
  val anonymousDataCollection: Boolean?,
  val news: Boolean?,
  val securityUpdates: Boolean?,
) {
  fun isAnonymousDataCollection(): Boolean = anonymousDataCollection == true

  fun isNews(): Boolean = news == true

  fun isSecurityUpdates(): Boolean = securityUpdates == true
}
