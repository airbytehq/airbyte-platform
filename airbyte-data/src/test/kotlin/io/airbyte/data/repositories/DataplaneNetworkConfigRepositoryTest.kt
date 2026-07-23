/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataplaneNetworkConfig
import io.airbyte.db.instance.configs.jooq.generated.enums.CloudProvider
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
class DataplaneNetworkConfigRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    private val organizationId = UUID.randomUUID()
    private val dataplaneGroupId = UUID.randomUUID()

    private const val AWS_CONFIG =
      """{"region":"us-east-1","vpcId":"vpc-123","subnetIds":["subnet-1"],"securityGroupIds":["sg-1"]}"""

    @BeforeAll
    @JvmStatic
    fun setup() {
      jooqDslContext
        .insertInto(
          org.jooq.impl.DSL
            .table("organization"),
          org.jooq.impl.DSL
            .field("id"),
          org.jooq.impl.DSL
            .field("name"),
          org.jooq.impl.DSL
            .field("email"),
        ).values(organizationId, "test-org", "test@test.com")
        .execute()

      jooqDslContext
        .insertInto(
          org.jooq.impl.DSL
            .table("dataplane_group"),
          org.jooq.impl.DSL
            .field("id"),
          org.jooq.impl.DSL
            .field("organization_id"),
          org.jooq.impl.DSL
            .field("name"),
        ).values(dataplaneGroupId, organizationId, "test-group")
        .execute()
    }
  }

  @AfterEach
  fun tearDown() {
    dataplaneNetworkConfigRepository.deleteAll()
  }

  @Test
  fun `test create and retrieve by dataplane group id`() {
    val saved =
      dataplaneNetworkConfigRepository.save(
        DataplaneNetworkConfig(
          dataplaneGroupId = dataplaneGroupId,
          provider = CloudProvider.aws,
          config = AWS_CONFIG,
        ),
      )

    assertNotNull(saved.id)

    val retrieved = dataplaneNetworkConfigRepository.findByDataplaneGroupId(dataplaneGroupId).get()
    assertEquals(dataplaneGroupId, retrieved.dataplaneGroupId)
    assertEquals(CloudProvider.aws, retrieved.provider)
    assertTrue(retrieved.config.contains("vpc-123"))
  }

  @Test
  fun `test find by dataplane group id returns empty when none exist`() {
    val result = dataplaneNetworkConfigRepository.findByDataplaneGroupId(UUID.randomUUID())
    assertTrue(result.isEmpty)
  }

  @Test
  fun `test update config`() {
    val saved =
      dataplaneNetworkConfigRepository.save(
        DataplaneNetworkConfig(
          dataplaneGroupId = dataplaneGroupId,
          provider = CloudProvider.aws,
          config = AWS_CONFIG,
        ),
      )

    val updatedConfig =
      """{"region":"us-east-1","vpcId":"vpc-456","subnetIds":["subnet-2"],"securityGroupIds":["sg-2"]}"""
    saved.config = updatedConfig
    val updated = dataplaneNetworkConfigRepository.update(saved)
    assertTrue(updated.config.contains("vpc-456"))
  }

  @Test
  fun `test delete by id`() {
    val saved =
      dataplaneNetworkConfigRepository.save(
        DataplaneNetworkConfig(
          dataplaneGroupId = dataplaneGroupId,
          provider = CloudProvider.aws,
          config = AWS_CONFIG,
        ),
      )

    dataplaneNetworkConfigRepository.deleteById(saved.id!!)
    assertTrue(dataplaneNetworkConfigRepository.findById(saved.id!!).isEmpty)
  }
}
