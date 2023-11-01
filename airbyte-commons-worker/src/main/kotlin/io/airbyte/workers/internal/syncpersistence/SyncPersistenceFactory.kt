package io.airbyte.workers.internal.syncpersistence

import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.micronaut.context.ApplicationContext
import io.micronaut.kotlin.context.createBean
import jakarta.inject.Singleton
import java.util.UUID

/**
 * A Factory for SyncPersistence.
 * <p>
 * Because we currently have two execution path with two different lifecycles, one being the
 * duration of a sync, the other duration of a worker. We introduce a Factory to keep an explicit
 * control of the lifecycle of this bean.
 * <p>
 * We use this factory rather to avoid having to directly inject ApplicationContext into classes
 * that would need to instantiate a SyncPersistence.
 */
@Singleton
class SyncPersistenceFactory(private val applicationContext: ApplicationContext) {
  /**
   * Get an instance of SyncPersistence
   */
  fun get(
    connectionId: UUID,
    jobId: Long,
    attemptNumber: Int,
    catalog: ConfiguredAirbyteCatalog,
  ): SyncPersistence = applicationContext.createBean(connectionId, jobId, attemptNumber, catalog)
}
