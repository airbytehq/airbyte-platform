/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.ConnectionContext
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.FusionLaunchEnabled
import io.airbyte.featureflag.TestClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.authn.DataplaneIdentityService
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.mockk.every
import io.mockk.mockk
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class FusionLaunchResolverTest {
  private companion object {
    const val ACCOUNT_ID = "123456789012"
  }

  private val group = UUID.randomUUID()
  private val organization = UUID.randomUUID()
  private val identity: DataplaneIdentityService =
    mockk {
      every { authNDrivenDataplaneConfig } returns DataplaneConfig(UUID.randomUUID(), "dp", true, group, "group", null)
    }
  private val payload =
    SyncPayload(
      ReplicationInput()
        .withWorkspaceId(UUID.randomUUID())
        .withConnectionId(UUID.randomUUID())
        .withSourceId(UUID.randomUUID())
        .withDestinationId(UUID.randomUUID())
        .withConnectionContext(ConnectionContext().withOrganizationId(organization)),
    )

  private fun resolver(
    server: MockWebServer,
    edition: AirbyteEdition = AirbyteEdition.CLOUD,
    featureFlagClient: FeatureFlagClient = TestClient(mapOf(FusionLaunchEnabled.key to true)),
  ): FusionLaunchResolver =
    FusionLaunchResolver(
      AirbyteApiClient(server.url("/api").toString(), RetryPolicy.ofDefaults(), OkHttpClient()),
      identity,
      featureFlagClient,
      edition,
    )

  private fun body(
    enabled: Boolean = true,
    extra: Map<String, String> = emptyMap(),
    accountId: String = ACCOUNT_ID,
  ): String =
    Jsons.serialize(
      mapOf<String, Any>(
        "reason" to if (enabled) "enabled" else "not_enabled",
        "injectAwsBootstrapCredentials" to enabled,
        "destinationEnvironment" to
          if (enabled) {
            mapOf(
              "AIRBYTE_FUSION_ENABLED" to "true",
              "AIRBYTE_FUSION_S3_BUCKET" to "fake-fusion-bucket",
              "AIRBYTE_FUSION_S3_REGION" to "us-west-2",
              "AIRBYTE_FUSION_S3_PREFIX" to "fusion",
              "AIRBYTE_FUSION_S3_ROLE_ARN" to "arn:aws:iam::$accountId:role/airbyte-fusion-writer-$organization",
            ) + extra
          } else {
            emptyMap()
          },
      ),
    )

  @Test
  fun `other editions and disabled rollout do not call resolver`() {
    MockWebServer().use { server ->
      payload.fusionDestinationEnvironment = mapOf("AIRBYTE_FUSION_ENABLED" to "true")
      for (edition in AirbyteEdition.entries.filter { it != AirbyteEdition.CLOUD }) {
        resolver(server, edition).resolve(payload, "workload")
        resolver(server, edition = edition).resolve(payload, "workload")
      }
      resolver(server, featureFlagClient = TestClient()).resolve(payload, "workload")
      val missingContextPayload = SyncPayload(Jsons.clone(payload.input).withConnectionContext(null))
      resolver(server, featureFlagClient = TestClient()).resolve(missingContextPayload, "workload")
      assertEquals(0, server.requestCount)
      assertTrue(payload.fusionDestinationEnvironment.isEmpty())
      assertTrue(missingContextPayload.fusionDestinationEnvironment.isEmpty())
    }
  }

  @Test
  fun `missing organization context logs and skips Fusion resolution`() {
    MockWebServer().use { server ->
      val missingOrganizationPayload = SyncPayload(Jsons.clone(payload.input))
      requireNotNull(missingOrganizationPayload.input.connectionContext).organizationId = null
      missingOrganizationPayload.fusionDestinationEnvironment = mapOf("AIRBYTE_FUSION_ENABLED" to "true")
      resolver(server).resolve(missingOrganizationPayload, "workload")
      assertEquals(0, server.requestCount)
      assertTrue(missingOrganizationPayload.fusionDestinationEnvironment.isEmpty())

      val missingContextPayload = SyncPayload(Jsons.clone(payload.input).withConnectionContext(null))
      resolver(server).resolve(missingContextPayload, "workload")
      assertEquals(0, server.requestCount)
      assertTrue(missingContextPayload.fusionDestinationEnvironment.isEmpty())
    }
  }

  @Test
  fun `accepts server account and validates organization writer role`() {
    MockWebServer().use { server ->
      val serverAccount = "111111111111"
      server.enqueue(MockResponse().setBody(body(accountId = serverAccount)).setHeader("Content-Type", "application/json"))
      resolver(server).resolve(payload, "workload")
      assertEquals(
        "arn:aws:iam::$serverAccount:role/airbyte-fusion-writer-$organization",
        payload.fusionDestinationEnvironment["AIRBYTE_FUSION_S3_ROLE_ARN"],
      )
    }
  }

  @Test
  fun `resolves current facts and clears revoked enablement with workload identities`() {
    MockWebServer().use { server ->
      server.enqueue(MockResponse().setBody(body()).setHeader("Content-Type", "application/json"))
      server.enqueue(MockResponse().setBody(body(false)).setHeader("Content-Type", "application/json"))
      val resolver = resolver(server)
      resolver.resolve(payload, "workload")
      assertEquals(5, payload.fusionDestinationEnvironment.size)
      val request = server.takeRequest()
      assertEquals("POST", request.method)
      assertEquals("/api/v1/connections/resolve_enablement_for_sync", request.path)
      val json = Jsons.deserialize(request.body.readUtf8())
      assertEquals(group.toString(), json["dataplaneGroupId"].asText())
      assertEquals(organization.toString(), json["organizationId"].asText())
      assertEquals(payload.input.sourceId.toString(), json["sourceId"].asText())
      assertEquals("workload", json["workloadId"].asText())
      resolver.resolve(payload, "workload")
      assertTrue(payload.fusionDestinationEnvironment.isEmpty())
    }
  }

  @Test
  fun `transport and policy failures propagate instead of disabling copy`() {
    MockWebServer().use { server ->
      server.enqueue(MockResponse().setResponseCode(409))
      assertThrows<Exception> { resolver(server).resolve(payload, "workload") }
      assertEquals(1, server.requestCount)
      assertTrue(payload.fusionDestinationEnvironment.isEmpty())
    }
  }

  @Test
  fun `rejects malformed enabled responses and wrong organization role`() {
    val invalid =
      listOf(
        "{}",
        body().replace("airbyte-fusion-writer-$organization", "airbyte-fusion-writer-${UUID.randomUUID()}"),
        body().replace("arn:aws:iam::", "arn:aws-cn:iam::"),
        body().replace(":iam::", ":s3::"),
        body().replace(":$ACCOUNT_ID:", ":12345:"),
        body().replace(":role/airbyte-fusion-writer-", ":role/path/airbyte-fusion-writer-"),
        body().replace("\"injectAwsBootstrapCredentials\":true", "\"injectAwsBootstrapCredentials\":false"),
        body().replace("\"AIRBYTE_FUSION_S3_REGION\":\"us-west-2\"", "\"AWS_SECRET_ACCESS_KEY\":\"bad\""),
        body().replace("\"AIRBYTE_FUSION_S3_PREFIX\":\"fusion\"", "\"AIRBYTE_FUSION_S3_PREFIX\":\"\""),
        body().replace("\"reason\":\"enabled\"", "\"reason\":\"not_enabled\""),
        // Legacy connector settings do not satisfy the Fusion contract.
        body().replace("AIRBYTE_FUSION_ENABLED", "AIRBYTE_S3_COPY_ENABLED"),
        body().replace("AIRBYTE_FUSION_S3_BUCKET", "AIRBYTE_S3_COPY_BUCKET"),
        // Missing a required key.
        body().replace("\"AIRBYTE_FUSION_S3_REGION\":\"us-west-2\",", ""),
        // Unknown extra keys, including one under the copy prefix.
        body(extra = mapOf("AIRBYTE_FUSION_S3_ORGANIZATION_ID" to organization.toString())),
        body(extra = mapOf("AWS_ACCESS_KEY_ID" to "bad")),
      )
    for (response in invalid) {
      MockWebServer().use { server ->
        server.enqueue(MockResponse().setBody(response).setHeader("Content-Type", "application/json"))
        assertThrows<Exception> { resolver(server).resolve(payload, "workload") }
        assertTrue(payload.fusionDestinationEnvironment.isEmpty())
      }
    }
  }

  @Test
  fun `accepts and forwards an optional http endpoint override`() {
    for (endpoint in listOf("http://localstack:4566", "https://s3.localhost.localstack.cloud:4566/")) {
      MockWebServer().use { server ->
        server.enqueue(
          MockResponse()
            .setBody(body(extra = mapOf("AIRBYTE_S3_COPY_ENDPOINT" to endpoint)))
            .setHeader("Content-Type", "application/json"),
        )
        resolver(server).resolve(payload, "workload")
        assertEquals(6, payload.fusionDestinationEnvironment.size)
        assertEquals(endpoint, payload.fusionDestinationEnvironment["AIRBYTE_S3_COPY_ENDPOINT"])
        assertEquals("true", payload.fusionDestinationEnvironment["AIRBYTE_FUSION_ENABLED"])
      }
    }
  }

  @Test
  fun `rejects invalid endpoint overrides`() {
    for (endpoint in listOf("", " ", "localstack:4566", "ftp://localstack:4566", "http://", "http:///path", "not a url", "s3.amazonaws.com")) {
      MockWebServer().use { server ->
        server.enqueue(
          MockResponse()
            .setBody(body(extra = mapOf("AIRBYTE_S3_COPY_ENDPOINT" to endpoint)))
            .setHeader("Content-Type", "application/json"),
        )
        assertThrows<IllegalArgumentException> { resolver(server).resolve(payload, "workload") }
        assertTrue(payload.fusionDestinationEnvironment.isEmpty())
      }
    }
  }
}
