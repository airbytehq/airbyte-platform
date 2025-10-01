/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.metrics

import io.airbyte.config.WorkloadConstants.Companion.PUBLIC_ORG_ID
import io.airbyte.metrics.lib.MetricTags
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Tags
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class PrettifyDataplaneMetricTagsMeterFilterTest {
  @Test
  fun `map adds dataplane name tag when dataplane ID is present and name tag is missing`() {
    val dataplaneId = UUID.randomUUID()
    val dataplaneName = "test-dataplane"

    val cache =
      mockk<MetricTagsPrettifierCache> {
        every { dataplaneNameById(dataplaneId) } returns dataplaneName
      }

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val meterId =
      Meter.Id(
        "test.metric",
        Tags.of(MetricTags.DATA_PLANE_ID_TAG, dataplaneId.toString()),
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify dataplane name tag was added
    val nameTag = result.getTag(MetricTags.DATA_PLANE_NAME_TAG)
    assertNotNull(nameTag)
    assertEquals(dataplaneName, nameTag)

    // Verify cache was called
    verify(exactly = 1) { cache.dataplaneNameById(dataplaneId) }
  }

  @Test
  fun `map does not add dataplane name tag when name tag already exists`() {
    val dataplaneId = UUID.randomUUID()
    val existingName = "existing-dataplane-name"

    val cache = mockk<MetricTagsPrettifierCache>()

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val meterId =
      Meter.Id(
        "test.metric",
        Tags
          .of(MetricTags.DATA_PLANE_ID_TAG, dataplaneId.toString())
          .and(MetricTags.DATA_PLANE_NAME_TAG, existingName),
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify the existing name tag is preserved
    assertEquals(existingName, result.getTag(MetricTags.DATA_PLANE_NAME_TAG))

    // Verify cache was not called since tag already exists
    verify(exactly = 0) { cache.dataplaneNameById(any()) }
  }

  @Test
  fun `map adds dataplane group name tag when group ID is present and name tag is missing`() {
    val groupId = UUID.randomUUID()
    val groupName = "test-group"
    val orgId = UUID.randomUUID()

    val cache =
      mockk<MetricTagsPrettifierCache> {
        every { dataplaneGroupNameById(groupId) } returns groupName
        every { orgIdForDataplaneGroupId(groupId) } returns orgId
      }

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val meterId =
      Meter.Id(
        "test.metric",
        Tags.of(MetricTags.DATA_PLANE_GROUP_TAG, groupId.toString()),
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify group name tag was added
    assertEquals(groupName, result.getTag(MetricTags.DATA_PLANE_GROUP_NAME_TAG))

    // Verify visibility tag was added
    assertEquals(MetricTags.PRIVATE, result.getTag(MetricTags.DATA_PLANE_VISIBILITY))

    verify(exactly = 1) { cache.dataplaneGroupNameById(groupId) }
    verify(exactly = 1) { cache.orgIdForDataplaneGroupId(groupId) }
  }

  @Test
  fun `map does not add dataplane group name tag when name tag already exists`() {
    val groupId = UUID.randomUUID()
    val existingName = "existing-group-name"
    val orgId = UUID.randomUUID()

    val cache =
      mockk<MetricTagsPrettifierCache> {
        every { orgIdForDataplaneGroupId(groupId) } returns orgId
      }

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val meterId =
      Meter.Id(
        "test.metric",
        Tags
          .of(MetricTags.DATA_PLANE_GROUP_TAG, groupId.toString())
          .and(MetricTags.DATA_PLANE_GROUP_NAME_TAG, existingName),
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify the existing name tag is preserved
    assertEquals(existingName, result.getTag(MetricTags.DATA_PLANE_GROUP_NAME_TAG))

    // Verify visibility tag was still added
    assertEquals(MetricTags.PRIVATE, result.getTag(MetricTags.DATA_PLANE_VISIBILITY))

    // Verify cache was not called for group name
    verify(exactly = 0) { cache.dataplaneGroupNameById(any()) }
  }

  @Test
  fun `map adds public visibility tag when org is PUBLIC_ORG_ID`() {
    val groupId = UUID.randomUUID()
    val groupName = "public-group"

    val cache =
      mockk<MetricTagsPrettifierCache> {
        every { dataplaneGroupNameById(groupId) } returns groupName
        every { orgIdForDataplaneGroupId(groupId) } returns PUBLIC_ORG_ID
      }

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val meterId =
      Meter.Id(
        "test.metric",
        Tags.of(MetricTags.DATA_PLANE_GROUP_TAG, groupId.toString()),
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify visibility is public
    assertEquals(MetricTags.PUBLIC, result.getTag(MetricTags.DATA_PLANE_VISIBILITY))
  }

  @Test
  fun `map adds private visibility tag when org is not PUBLIC_ORG_ID`() {
    val groupId = UUID.randomUUID()
    val groupName = "private-group"
    val privateOrgId = UUID.randomUUID()

    val cache =
      mockk<MetricTagsPrettifierCache> {
        every { dataplaneGroupNameById(groupId) } returns groupName
        every { orgIdForDataplaneGroupId(groupId) } returns privateOrgId
      }

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val meterId =
      Meter.Id(
        "test.metric",
        Tags.of(MetricTags.DATA_PLANE_GROUP_TAG, groupId.toString()),
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify visibility is private
    assertEquals(MetricTags.PRIVATE, result.getTag(MetricTags.DATA_PLANE_VISIBILITY))
  }

  @Test
  fun `map does not add visibility tag when org ID is null`() {
    val groupId = UUID.randomUUID()
    val groupName = "group-no-org"

    val cache =
      mockk<MetricTagsPrettifierCache> {
        every { dataplaneGroupNameById(groupId) } returns groupName
        every { orgIdForDataplaneGroupId(groupId) } returns null
      }

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val meterId =
      Meter.Id(
        "test.metric",
        Tags.of(MetricTags.DATA_PLANE_GROUP_TAG, groupId.toString()),
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify visibility tag was not added
    val visibilityTag = result.getTag(MetricTags.DATA_PLANE_VISIBILITY)
    assertEquals(null, visibilityTag)
  }

  @Test
  fun `map handles invalid dataplane ID gracefully`() {
    val cache = mockk<MetricTagsPrettifierCache>()

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val meterId =
      Meter.Id(
        "test.metric",
        Tags.of(MetricTags.DATA_PLANE_ID_TAG, "undefined"),
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify the meter ID is returned unchanged (no tags added)
    assertEquals(meterId.tags.size, result.tags.size)
    assertEquals("undefined", result.getTag(MetricTags.DATA_PLANE_ID_TAG))

    // Verify cache was not called
    verify(exactly = 0) { cache.dataplaneNameById(any()) }
  }

  @Test
  fun `map handles invalid dataplane group ID gracefully`() {
    val cache = mockk<MetricTagsPrettifierCache>()

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val meterId =
      Meter.Id(
        "test.metric",
        Tags.of(MetricTags.DATA_PLANE_GROUP_TAG, "not-a-uuid"),
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify the meter ID is returned unchanged
    assertEquals(meterId.tags.size, result.tags.size)
    assertEquals("not-a-uuid", result.getTag(MetricTags.DATA_PLANE_GROUP_TAG))

    // Verify cache was not called
    verify(exactly = 0) { cache.dataplaneGroupNameById(any()) }
  }

  @Test
  fun `map returns unchanged meter ID when no relevant tags are present`() {
    val cache = mockk<MetricTagsPrettifierCache>()

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val meterId =
      Meter.Id(
        "test.metric",
        Tags.of("other.tag", "value"),
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify the meter ID is returned unchanged
    assertEquals(meterId, result)
    assertEquals(1, result.tags.size)

    // Verify cache was not called
    verify(exactly = 0) { cache.dataplaneNameById(any()) }
    verify(exactly = 0) { cache.dataplaneGroupNameById(any()) }
  }

  @Test
  fun `map adds both dataplane and dataplane group tags when both IDs are present`() {
    val dataplaneId = UUID.randomUUID()
    val dataplaneName = "test-dataplane"
    val groupId = UUID.randomUUID()
    val groupName = "test-group"
    val orgId = UUID.randomUUID()

    val cache =
      mockk<MetricTagsPrettifierCache> {
        every { dataplaneNameById(dataplaneId) } returns dataplaneName
        every { dataplaneGroupNameById(groupId) } returns groupName
        every { orgIdForDataplaneGroupId(groupId) } returns orgId
      }

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val meterId =
      Meter.Id(
        "test.metric",
        Tags
          .of(MetricTags.DATA_PLANE_ID_TAG, dataplaneId.toString())
          .and(MetricTags.DATA_PLANE_GROUP_TAG, groupId.toString()),
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify both tags were added
    assertEquals(dataplaneName, result.getTag(MetricTags.DATA_PLANE_NAME_TAG))
    assertEquals(groupName, result.getTag(MetricTags.DATA_PLANE_GROUP_NAME_TAG))
    assertEquals(MetricTags.PRIVATE, result.getTag(MetricTags.DATA_PLANE_VISIBILITY))

    verify(exactly = 1) { cache.dataplaneNameById(dataplaneId) }
    verify(exactly = 1) { cache.dataplaneGroupNameById(groupId) }
    verify(exactly = 1) { cache.orgIdForDataplaneGroupId(groupId) }
  }

  @Test
  fun `getDataplaneVisibility returns PUBLIC for PUBLIC_ORG_ID`() {
    val cache = mockk<MetricTagsPrettifierCache>()
    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val visibility = filter.getDataplaneVisibility(PUBLIC_ORG_ID)

    assertEquals(MetricTags.PUBLIC, visibility)
  }

  @Test
  fun `getDataplaneVisibility returns PRIVATE for non-public org ID`() {
    val cache = mockk<MetricTagsPrettifierCache>()
    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val visibility = filter.getDataplaneVisibility(UUID.randomUUID())

    assertEquals(MetricTags.PRIVATE, visibility)
  }

  @Test
  fun `map preserves all original tags`() {
    val dataplaneId = UUID.randomUUID()
    val dataplaneName = "test-dataplane"

    val cache =
      mockk<MetricTagsPrettifierCache> {
        every { dataplaneNameById(dataplaneId) } returns dataplaneName
      }

    val filter = PrettifyDataplaneMetricTagsMeterFilterBuilder.PrettifyDataplaneMetricTagsMeterFilter(cache)

    val originalTags =
      Tags
        .of("custom.tag", "custom-value")
        .and("another.tag", "another-value")
        .and(MetricTags.DATA_PLANE_ID_TAG, dataplaneId.toString())

    val meterId =
      Meter.Id(
        "test.metric",
        originalTags,
        null,
        null,
        Meter.Type.COUNTER,
      )

    val result = filter.map(meterId)

    // Verify all original tags are preserved
    assertTrue(result.tags.any { it.key == "custom.tag" && it.value == "custom-value" })
    assertTrue(result.tags.any { it.key == "another.tag" && it.value == "another-value" })
    assertTrue(result.tags.any { it.key == MetricTags.DATA_PLANE_ID_TAG && it.value == dataplaneId.toString() })

    // Verify new tag was added
    assertTrue(result.tags.any { it.key == MetricTags.DATA_PLANE_NAME_TAG && it.value == dataplaneName })
  }
}
