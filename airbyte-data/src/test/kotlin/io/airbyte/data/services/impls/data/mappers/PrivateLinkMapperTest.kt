/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.domain.models.PrivateLink
import io.airbyte.domain.models.PrivateLinkServiceConfig
import io.airbyte.domain.models.PrivateLinkServiceType
import io.airbyte.domain.models.PrivateLinkStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID
import io.airbyte.data.repositories.entities.PrivateLink as EntityPrivateLink
import io.airbyte.db.instance.configs.jooq.generated.enums.PrivateLinkServiceType as EntityPrivateLinkServiceType
import io.airbyte.db.instance.configs.jooq.generated.enums.PrivateLinkStatus as EntityPrivateLinkStatus

class PrivateLinkMapperTest {
  private val workspaceId = UUID.randomUUID()
  private val dataplaneGroupId = UUID.randomUUID()

  @Test
  fun `endpoint variant round-trips through toEntity then toDomainModel`() {
    val domain =
      PrivateLink(
        id = UUID.randomUUID(),
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        name = "endpoint-link",
        status = PrivateLinkStatus.AVAILABLE,
        serviceRegion = "us-east-1",
        serviceName = "com.amazonaws.vpce.us-east-1.vpce-svc-abc",
        serviceType = PrivateLinkServiceType.ENDPOINT,
        serviceConfig =
          PrivateLinkServiceConfig.Endpoint(
            name = "com.amazonaws.vpce.us-east-1.vpce-svc-abc",
            region = "us-east-1",
          ),
      )

    val roundTripped = domain.toEntity().toDomainModel()

    assertEquals(domain, roundTripped)
  }

  @Test
  fun `storage variant round-trips through toEntity then toDomainModel preserving optional bucket`() {
    val domain =
      PrivateLink(
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        name = "storage-link",
        status = PrivateLinkStatus.CREATING,
        serviceRegion = "us-east-1",
        serviceName = "ignored-for-storage",
        serviceType = PrivateLinkServiceType.STORAGE,
        serviceConfig =
          PrivateLinkServiceConfig.Storage(
            region = "us-east-2",
            bucket = "my-bucket",
          ),
      )

    val roundTripped = domain.toEntity().toDomainModel()

    assertEquals(domain.serviceType, roundTripped.serviceType)
    assertEquals(domain.serviceConfig, roundTripped.serviceConfig)
  }

  @Test
  fun `storage variant with null bucket round-trips`() {
    val domain =
      PrivateLink(
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        name = "storage-link",
        status = PrivateLinkStatus.CREATING,
        serviceRegion = "us-east-1",
        serviceName = "ignored-for-storage",
        serviceType = PrivateLinkServiceType.STORAGE,
        serviceConfig = PrivateLinkServiceConfig.Storage(region = "us-east-2", bucket = null),
      )

    val roundTripped = domain.toEntity().toDomainModel()

    assertEquals(PrivateLinkServiceConfig.Storage(region = "us-east-2", bucket = null), roundTripped.serviceConfig)
  }

  @Test
  fun `toDomainModel ignores unknown JSON fields like the migration backfill version stamp`() {
    val entity =
      EntityPrivateLink(
        id = UUID.randomUUID(),
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        name = "backfilled-link",
        status = EntityPrivateLinkStatus.available,
        serviceRegion = "us-east-1",
        serviceName = "com.amazonaws.vpce.us-east-1.vpce-svc-legacy",
        serviceType = EntityPrivateLinkServiceType.endpoint,
        serviceConfig =
          """{"version":1,"name":"com.amazonaws.vpce.us-east-1.vpce-svc-legacy","region":"us-east-1"}""",
      )

    val domain = entity.toDomainModel()

    val endpoint = domain.serviceConfig as PrivateLinkServiceConfig.Endpoint
    assertEquals("com.amazonaws.vpce.us-east-1.vpce-svc-legacy", endpoint.name)
    assertEquals("us-east-1", endpoint.region)
  }

  @Test
  fun `service_type enum maps both directions`() {
    assertEquals(EntityPrivateLinkServiceType.endpoint, PrivateLinkServiceType.ENDPOINT.toEntityEnum())
    assertEquals(EntityPrivateLinkServiceType.storage, PrivateLinkServiceType.STORAGE.toEntityEnum())
    assertEquals(PrivateLinkServiceType.ENDPOINT, EntityPrivateLinkServiceType.endpoint.toDomainEnum())
    assertEquals(PrivateLinkServiceType.STORAGE, EntityPrivateLinkServiceType.storage.toDomainEnum())
  }

  @Test
  fun `toEntity serializes endpoint serviceConfig as expected JSON shape`() {
    val domain =
      PrivateLink(
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        name = "endpoint-link",
        status = PrivateLinkStatus.CREATING,
        serviceRegion = "us-west-2",
        serviceName = "com.amazonaws.vpce.us-west-2.vpce-svc-xyz",
        serviceType = PrivateLinkServiceType.ENDPOINT,
        serviceConfig =
          PrivateLinkServiceConfig.Endpoint(
            name = "com.amazonaws.vpce.us-west-2.vpce-svc-xyz",
            region = "us-west-2",
          ),
      )

    val json = domain.toEntity().serviceConfig

    assertTrue(json.contains("\"name\":\"com.amazonaws.vpce.us-west-2.vpce-svc-xyz\""))
    assertTrue(json.contains("\"region\":\"us-west-2\""))
  }
}
