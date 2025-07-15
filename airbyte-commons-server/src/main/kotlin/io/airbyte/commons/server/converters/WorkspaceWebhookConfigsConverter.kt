/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.WebhookConfigRead
import io.airbyte.api.model.generated.WebhookConfigWrite
import io.airbyte.commons.json.Jsons
import io.airbyte.config.WebhookConfig
import io.airbyte.config.WebhookOperationConfigs
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Collectors

/**
 * Convert between API and internal versions of notification models.
 *
 * NOTE: we suppress this warning because PMD thinks it can be a foreach loop in toApiReads but the
 * compiler disagrees.
 */
object WorkspaceWebhookConfigsConverter {
  @JvmStatic
  fun toPersistenceWrite(
    apiWebhookConfigs: List<WebhookConfigWrite>?,
    uuidSupplier: Supplier<UUID>,
  ): JsonNode {
    if (apiWebhookConfigs == null) {
      return Jsons.emptyObject()
    }

    val configs =
      WebhookOperationConfigs()
        .withWebhookConfigs(
          apiWebhookConfigs
            .stream()
            .map { item: WebhookConfigWrite -> toPersistenceConfig(uuidSupplier, item) }
            .collect(Collectors.toList()),
        )

    return Jsons.jsonNode(configs)
  }

  /**
   * Extract the read-only properties from a set of persisted webhook operation configs.
   *
   *
   * Specifically, returns the id and name but excludes the secret auth token. Note that we "manually"
   * deserialize the JSON tree instead of deserializing to our internal schema --
   * WebhookOperationConfigs -- because the persisted JSON doesn't conform to that schema until we
   * hydrate the secrets. Since we don't want to unnecessarily hydrate the secrets to read from the
   * API, we do this instead.
   *
   *
   * TODO(mfsiega-airbyte): try find a cleaner way to handle this situation.
   *
   * @param persistedWebhookConfig - The JsonNode of the persisted webhook configs
   * @return a list of (webhook id, name) pairs
   */
  fun toApiReads(persistedWebhookConfig: JsonNode?): List<WebhookConfigRead> {
    if (persistedWebhookConfig == null) {
      return emptyList()
    }

    // NOTE: we deserialize it "by hand" because the secrets aren't hydrated, so we can't deserialize it
    // into the usual shape.
    // TODO(mfsiega-airbyte): find a cleaner way to handle this situation.
    val configReads: MutableList<WebhookConfigRead> = ArrayList()

    val configArray = persistedWebhookConfig.findPath("webhookConfigs")
    val it = configArray.elements()
    while (it.hasNext()) {
      val webhookConfig = it.next()
      configReads.add(toApiRead(webhookConfig))
    }
    return configReads
  }

  private fun toPersistenceConfig(
    uuidSupplier: Supplier<UUID>,
    input: WebhookConfigWrite,
  ): WebhookConfig =
    WebhookConfig()
      .withId(uuidSupplier.get())
      .withName(input.name)
      .withAuthToken(input.authToken)
      .withCustomDbtHost(input.customDbtHost)

  private fun toApiRead(configJson: JsonNode): WebhookConfigRead {
    val read = WebhookConfigRead()
    read.id = UUID.fromString(configJson.findValue("id").asText())
    read.name = configJson.findValue("name").asText()
    if (configJson.has("customDbtHost") && configJson.findValue("customDbtHost").isTextual) {
      read.customDbtHost = configJson.findValue("customDbtHost").asText()
    }
    return read
  }
}
