/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.analytics

import com.segment.analytics.Analytics
import com.segment.analytics.messages.AliasMessage
import com.segment.analytics.messages.IdentifyMessage
import com.segment.analytics.messages.TrackMessage
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs
import io.airbyte.config.Configs.WorkerEnvironment
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.context.env.Environment
import io.micronaut.http.context.ServerRequestContext
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Instant
import java.util.Optional
import java.util.UUID
import java.util.function.Function
import java.util.function.Supplier

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
  fun identify(workspaceId: UUID)

  fun alias(
    workspaceId: UUID,
    previousCustomerId: String?,
  )

  fun track(
    workspaceId: UUID,
    action: String?,
  )

  fun track(
    workspaceId: UUID,
    action: String?,
    metadata: Map<String?, Any?>?,
  )
}

/**
 * This class is a wrapper around the Segment backend Java SDK.
 * <p>
 * In general, the Segment SDK events have two pieces to them, a top-level userId field and a map of
 * properties.
 * <p>
 * As of 2021/11/03, the top level userId field is standardised on the
 * {@link StandardWorkspace#getCustomerId()} field. This field is a random UUID generated when a
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
@Requires(property = "airbyte.tracking-strategy", pattern = "(?i)^segment$")
@Named("trackingClient")
class SegmentTrackingClient(
  private val segmentAnalyticsClient: SegmentAnalyticsClient,
  private val trackingIdentityFetcher: TrackingIdentityFetcher,
  val deployment: Deployment,
  @Value("\${airbyte.role}") val airbyteRole: String,
) : TrackingClient {
  override fun identify(workspaceId: UUID) {
    val trackingIdentity: TrackingIdentity = trackingIdentityFetcher.apply(workspaceId)
    val identityMetadata: MutableMap<String, Any?> = HashMap()

    // deployment
    identityMetadata[AIRBYTE_VERSION_KEY] = trackingIdentity.airbyteVersion.serialize()
    identityMetadata["deployment_mode"] = deployment.deploymentMode
    identityMetadata["deployment_env"] = deployment.getDeploymentEnvironment()
    identityMetadata["deployment_id"] = deployment.deploymentIdSupplier.get()

    // workspace (includes info that in the future we would store in an organization)
    identityMetadata["anonymized"] = trackingIdentity.isAnonymousDataCollection()
    identityMetadata["subscribed_newsletter"] = trackingIdentity.isNews()
    identityMetadata["subscribed_security"] = trackingIdentity.isSecurityUpdates()
    if (trackingIdentity.email != null) {
      identityMetadata["email"] = trackingIdentity.email
    }

    // other
    if (airbyteRole.isNotBlank()) {
      identityMetadata[AIRBYTE_ROLE] = airbyteRole
    }

    val joinKey: String = trackingIdentity.customerId.toString()
    segmentAnalyticsClient.analyticsClient.enqueue(
      IdentifyMessage.builder() // user id is scoped by workspace. there is no cross-workspace tracking.
        .userId(joinKey)
        .traits(identityMetadata),
    )
  }

  override fun alias(
    workspaceId: UUID,
    previousCustomerId: String?,
  ) {
    val joinKey: String = trackingIdentityFetcher.apply(workspaceId).customerId.toString()
    segmentAnalyticsClient.analyticsClient.enqueue(AliasMessage.builder(previousCustomerId).userId(joinKey))
  }

  override fun track(
    workspaceId: UUID,
    action: String?,
  ) {
    track(workspaceId, action, emptyMap<String?, Any>())
  }

  override fun track(
    workspaceId: UUID,
    action: String?,
    metadata: Map<String?, Any?>?,
  ) {
    if (workspaceId == null) {
      logger.error { "Could not track action $action due to null workspaceId" }
      return
    }

    val mapCopy: MutableMap<String, Any?> = java.util.HashMap(metadata)
    val trackingIdentity: TrackingIdentity = trackingIdentityFetcher.apply(workspaceId)

    val airbyteSource: Optional<String> = getAirbyteSource()
    mapCopy[AIRBYTE_SOURCE] = airbyteSource.orElse(UNKNOWN)

    // Always add these traits.
    mapCopy[AIRBYTE_VERSION_KEY] = trackingIdentity.airbyteVersion.serialize()
    mapCopy[CUSTOMER_ID_KEY] = trackingIdentity.customerId
    mapCopy[AIRBYTE_DEPLOYMENT_ID] = deployment.deploymentIdSupplier.get()
    mapCopy[AIRBYTE_DEPLOYMENT_MODE] = deployment.deploymentMode
    mapCopy[AIRBYTE_TRACKED_AT] = Instant.now().toString()
    if (metadata!!.isNotEmpty()) {
      if (trackingIdentity.email != null) {
        mapCopy["email"] = trackingIdentity.email
      }
    }

    val joinKey: String = trackingIdentity.customerId.toString()
    segmentAnalyticsClient.analyticsClient.enqueue(
      TrackMessage.builder(action)
        .userId(joinKey)
        .properties(mapCopy),
    )
  }

  private fun getAirbyteSource(): Optional<String> {
    val currentRequest = ServerRequestContext.currentRequest<Any>()
    return if (currentRequest.isPresent) {
      Optional.ofNullable<String>(currentRequest.get().headers[AIRBYTE_ANALYTIC_SOURCE_HEADER])
    } else {
      Optional.empty<String>()
    }
  }

  companion object {
    const val AIRBYTE_ANALYTIC_SOURCE_HEADER = "X-Airbyte-Analytic-Source"
    const val AIRBYTE_DEPLOYMENT_ID = "deployment_id"
    const val AIRBYTE_DEPLOYMENT_MODE = "deployment_mode"
    const val AIRBYTE_ROLE = "airbyte_role"
    const val AIRBYTE_SOURCE = "airbyte_source"
    const val AIRBYTE_TRACKED_AT = "tracked_at"
    const val AIRBYTE_VERSION_KEY = "airbyte_version"
    const val CUSTOMER_ID_KEY = "user_id"
    const val UNKNOWN = "unknown"
  }
}

@Singleton
@Requires(property = "airbyte.tracking-strategy", pattern = "(?i)^segment$")
class SegmentAnalyticsClient {
  val analyticsClient: Analytics = Analytics.builder(SEGMENT_WRITE_KEY).build()

  companion object {
    const val SEGMENT_WRITE_KEY = "7UDdp5K55CyiGgsauOr2pNNujGvmhaeu"
  }
}

/**
 * Tracking client that logs to STDOUT. Mainly used for local development.
 */
@Singleton
@Requires(property = "airbyte.tracking-strategy", pattern = "(?i)^logging$")
@Named("trackingClient")
class LoggingTrackingClient(private val trackingIdentityFetcher: TrackingIdentityFetcher) : TrackingClient {
  override fun identify(workspaceId: UUID) {
    logger.info { "identify. userId: ${trackingIdentityFetcher.apply(workspaceId).customerId}" }
  }

  override fun alias(
    workspaceId: UUID,
    previousCustomerId: String?,
  ) {
    logger.info {
      "merge. userId: ${trackingIdentityFetcher.apply(workspaceId).customerId} previousUserId: $previousCustomerId"
    }
  }

  override fun track(
    workspaceId: UUID,
    action: String?,
  ) {
    track(workspaceId, action, emptyMap<String?, Any>())
  }

  override fun track(
    workspaceId: UUID,
    action: String?,
    metadata: Map<String?, Any?>?,
  ) {
    val trackingIdentity: TrackingIdentity = trackingIdentityFetcher.apply(workspaceId)
    val version: String = trackingIdentity.airbyteVersion.serialize()
    val userId: UUID = trackingIdentity.customerId
    logger.info { "track. version: $version, userId: $userId, action: $action, metadata: $metadata" }
  }
}

@Singleton
class TrackingIdentityFetcher(
  val airbyteVersion: AirbyteVersion,
  @Named("workspaceFetcher") val workspaceFetcher: Function<UUID, WorkspaceRead>,
) : Function<UUID, TrackingIdentity> {
  override fun apply(workspaceId: UUID): TrackingIdentity {
    val workspaceRead = workspaceFetcher.apply(workspaceId)
    val email: String? =
      if (workspaceRead.anonymousDataCollection != null && !workspaceRead.anonymousDataCollection!!) {
        workspaceRead.email
      } else {
        null
      }
    return TrackingIdentity(
      airbyteVersion,
      workspaceRead.customerId,
      email,
      workspaceRead.anonymousDataCollection,
      workspaceRead.news,
      workspaceRead.securityUpdates,
    )
  }
}

@Singleton
class Deployment(
  val deploymentMode: Configs.DeploymentMode,
  @Named("deploymentIdSupplier") val deploymentIdSupplier: Supplier<UUID>,
  val deploymentEnvironment: Environment,
) {
  fun getDeploymentEnvironment(): String {
    return if (deploymentEnvironment.activeNames.contains(
        Environment.KUBERNETES,
      )
    ) {
      WorkerEnvironment.KUBERNETES.name
    } else {
      WorkerEnvironment.DOCKER.name
    }
  }
}

data class TrackingIdentity(
  val airbyteVersion: AirbyteVersion,
  val customerId: UUID,
  val email: String?,
  val anonymousDataCollection: Boolean?,
  val news: Boolean?,
  val securityUpdates: Boolean?,
) {
  fun isAnonymousDataCollection(): Boolean {
    return anonymousDataCollection != null && anonymousDataCollection
  }

  fun isNews(): Boolean {
    return news != null && news
  }

  fun isSecurityUpdates(): Boolean {
    return securityUpdates != null && securityUpdates
  }
}
