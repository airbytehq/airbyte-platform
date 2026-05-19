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
  fun `toDomainModel reads endpoint variant from JSONB using the type discriminator`() {
    val entity =
      EntityPrivateLink(
        id = UUID.randomUUID(),
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        name = "endpoint-link",
        status = EntityPrivateLinkStatus.available,
        serviceConfig =
          """{"type":"endpoint","name":"com.amazonaws.vpce.us-east-1.vpce-svc-legacy","region":"us-east-1"}""",
      )

    val domain = entity.toDomainModel()

    val endpoint = domain.serviceConfig as PrivateLinkServiceConfig.Endpoint
    assertEquals("com.amazonaws.vpce.us-east-1.vpce-svc-legacy", endpoint.name)
    assertEquals("us-east-1", endpoint.region)
  }

  @Test
  fun `toDomainModel reads storage variant from JSONB using the type discriminator`() {
    val entity =
      EntityPrivateLink(
        id = UUID.randomUUID(),
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        name = "storage-link",
        status = EntityPrivateLinkStatus.available,
        serviceConfig =
          """{"type":"storage","region":"us-east-2","bucket":"my-bucket"}""",
      )

    val domain = entity.toDomainModel()

    val storage = domain.serviceConfig as PrivateLinkServiceConfig.Storage
    assertEquals("us-east-2", storage.region)
    assertEquals("my-bucket", storage.bucket)
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
        serviceConfig =
          """{"type":"endpoint","version":1,"name":"com.amazonaws.vpce.us-east-1.vpce-svc-legacy","region":"us-east-1"}""",
      )

    val domain = entity.toDomainModel()

    val endpoint = domain.serviceConfig as PrivateLinkServiceConfig.Endpoint
    assertEquals("com.amazonaws.vpce.us-east-1.vpce-svc-legacy", endpoint.name)
    assertEquals("us-east-1", endpoint.region)
  }

  @Test
  fun `toEntity serializes endpoint serviceConfig with type discriminator inside JSONB`() {
    val domain =
      PrivateLink(
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        name = "endpoint-link",
        status = PrivateLinkStatus.CREATING,
        serviceType = PrivateLinkServiceType.ENDPOINT,
        serviceConfig =
          PrivateLinkServiceConfig.Endpoint(
            name = "com.amazonaws.vpce.us-west-2.vpce-svc-xyz",
            region = "us-west-2",
          ),
      )

    val json = domain.toEntity().serviceConfig

    assertTrue(json.contains("\"type\":\"endpoint\""))
    assertTrue(json.contains("\"name\":\"com.amazonaws.vpce.us-west-2.vpce-svc-xyz\""))
    assertTrue(json.contains("\"region\":\"us-west-2\""))
    assertTrue(json.contains("\"version\":1"))
  }

  @Test
  fun `toEntity serializes storage serviceConfig with type discriminator inside JSONB`() {
    val domain =
      PrivateLink(
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        name = "storage-link",
        status = PrivateLinkStatus.CREATING,
        serviceType = PrivateLinkServiceType.STORAGE,
        serviceConfig = PrivateLinkServiceConfig.Storage(region = "us-east-2", bucket = "my-bucket"),
      )

    val json = domain.toEntity().serviceConfig

    assertTrue(json.contains("\"type\":\"storage\""))
    assertTrue(json.contains("\"region\":\"us-east-2\""))
    assertTrue(json.contains("\"bucket\":\"my-bucket\""))
    assertTrue(json.contains("\"version\":1"))
  }
}
