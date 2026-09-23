/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.workload.api.client.WorkloadApi
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import retrofit2.http.Body
import retrofit2.http.POST
import retrofit2.http.Path
import java.time.OffsetDateTime

class WorkloadApiContractTest {
  private val mapper = MoreMappers.initMapper()

  @Test
  fun `missing log delivery mode defaults to STANDARD`() {
    val workload = mapper.readValue("""{"id":"workload-id"}""", Workload::class.java)

    assertThat(workload.logDeliveryMode).isEqualTo(LogDeliveryMode.STANDARD)
    assertThat(LogDeliveryMode.entries).containsExactly(LogDeliveryMode.STANDARD, LogDeliveryMode.FLEX)
  }

  @Test
  fun `legacy workload mapper ignores the additive log delivery mode`() {
    val workload = mapper.readValue("""{"id":"workload-id","logDeliveryMode":"FLEX"}""", LegacyWorkload::class.java)

    assertThat(workload.id).isEqualTo("workload-id")
  }

  @Test
  fun `GCS authorization round trips through the discriminated union`() {
    val expiresAt = OffsetDateTime.parse("2026-09-02T12:34:56Z")
    val authorization: LogUploadAuthorization =
      GcsDownscopedOAuthLogUploadAuthorization(
        accessToken = "opaque-token",
        expiresAt = expiresAt,
        bucketName = "customer-logs",
        objectKeyPrefix = "workspace/workload/",
      )

    val json = mapper.writeValueAsString(authorization)
    val tree = mapper.readTree(json)
    val decoded = mapper.readValue(json, LogUploadAuthorization::class.java)

    assertThat(tree)
      .isEqualTo(
        mapper.readTree(
          """
          {
            "type": "GCS_DOWNSCOPED_OAUTH",
            "accessToken": "opaque-token",
            "expiresAt": "2026-09-02T12:34:56Z",
            "bucketName": "customer-logs",
            "objectKeyPrefix": "workspace/workload/"
          }
          """.trimIndent(),
        ),
      )
    assertThat(decoded).isEqualTo(authorization)
  }

  @Test
  fun `GCS authorization string representation redacts its access token`() {
    val authorization =
      GcsDownscopedOAuthLogUploadAuthorization(
        accessToken = "opaque-secret-token",
        expiresAt = OffsetDateTime.parse("2026-09-02T12:34:56Z"),
        bucketName = "customer-logs",
        objectKeyPrefix = "job-logging/job/7/attempt/2/",
      )

    assertThat(authorization.toString())
      .doesNotContain("opaque-secret-token")
      .contains("accessToken=******")
  }

  @Test
  fun `authorization route is bodyless and contains only the workload path parameter`() {
    val method = WorkloadApi::class.java.getMethod("workloadLogUploadAuthorization", String::class.java)

    assertThat(method.getAnnotation(POST::class.java).value).isEqualTo("{workloadId}/log-upload-authorization")
    assertThat(
      method.parameterAnnotations
        .single()
        .filterIsInstance<Path>()
        .single()
        .value,
    ).isEqualTo("workloadId")
    assertThat(method.parameterAnnotations.single().filterIsInstance<Body>()).isEmpty()
  }

  @Test
  fun `request models do not expose server-derived log delivery or authorization scope`() {
    val createRequestJson = mapper.valueToTree<JsonNode>(WorkloadCreateRequest())

    assertThat(createRequestJson.has("logDeliveryMode")).isFalse()
    assertThat(createRequestJson.has("bucketName")).isFalse()
    assertThat(createRequestJson.has("objectKeyPrefix")).isFalse()
    assertThat(createRequestJson.has("expiresAt")).isFalse()
  }

  private data class LegacyWorkload(
    val id: String = "",
  )
}
