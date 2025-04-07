/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.bootloader.K8sSecretHelper.base64Encode
import io.fabric8.kubernetes.api.model.Secret
import io.fabric8.kubernetes.api.model.SecretBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.NamespaceableResource
import io.kotest.assertions.asClue
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Test

class K8sSecretHelperTest {
  @Test
  fun `should create secret when it does not exist`() {
    val kubernetesClient = mockk<KubernetesClient>()

    val creator =
      mockk<NamespaceableResource<Secret>> {
        every { create() } returns mockk {}
      }

    val resourceSlot = slot<Secret>()

    every { kubernetesClient.secrets() } returns
      mockk {
        every { withName("my-secret") } returns
          mockk {
            every { get() } returns null
          }
      }
    every { kubernetesClient.resource(capture(resourceSlot)) } returns creator

    val secretData = mapOf("key1" to "value1")

    K8sSecretHelper.createOrUpdateSecret(kubernetesClient, "my-secret", secretData)

    verify { creator.create() }
    resourceSlot.captured.asClue {
      it.data.size shouldBe 1
      it.data["key1"] shouldBe base64Encode("value1")
    }
  }

  @Test
  fun `should update secret when it already exists`() {
    val kubernetesClient = mockk<KubernetesClient>()

    val updater =
      mockk<NamespaceableResource<Secret>> {
        every { update() } returns mockk {}
      }

    val resourceSlot = slot<Secret>()

    val existingSecret =
      SecretBuilder()
        .withNewMetadata()
        .withName("my-secret")
        .endMetadata()
        .addToData(
          mapOf(
            "key1" to "unchangedValue",
            "key2" to "oldValue",
          ),
        ).build()

    every { kubernetesClient.secrets() } returns
      mockk {
        every { withName("my-secret") } returns
          mockk {
            every { get() } returns existingSecret
          }
      }
    every { kubernetesClient.resource(capture(resourceSlot)) } returns updater

    val secretData = mapOf("key2" to "updatedValue", "key3" to "newValue")

    K8sSecretHelper.createOrUpdateSecret(kubernetesClient, "my-secret", secretData)

    verify { updater.update() }
    resourceSlot.captured.asClue {
      it.data.size shouldBe 3
      it.data["key1"] shouldBe "unchangedValue"
      it.data["key2"] shouldBe base64Encode("updatedValue")
      it.data["key3"] shouldBe base64Encode("newValue")
    }
  }
}
