/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.llm

import com.openai.client.OpenAIClient
import com.openai.client.okhttp.OpenAIOkHttpClient
import com.openai.models.ChatModel
import com.openai.models.chat.completions.ChatCompletionCreateParams
import datadog.trace.api.Trace
import io.micronaut.cache.annotation.Cacheable
import io.micronaut.context.annotation.ConfigurationProperties
import jakarta.inject.Singleton

@Singleton
open class OpenAIChatCompletionService(
  private val apiKeys: OpenAIProjectAPIKeyConfig,
) {
  private val clientCache: MutableMap<OpenAIProjectId, OpenAIClient> = mutableMapOf()

  private fun getClient(projectId: OpenAIProjectId): OpenAIClient {
    val apiKey =
      when (projectId) {
        OpenAIProjectId.FailedSyncAssistant -> apiKeys.failedSyncAssistant
      }

    return clientCache.computeIfAbsent(projectId) {
      OpenAIOkHttpClient
        .builder()
        .apiKey(apiKey)
        .responseValidation(true)
        .build()
    }
  }

  @Trace
  @Cacheable("openai-chat-completion-service")
  open fun getChatResponse(
    projectId: OpenAIProjectId,
    userPrompt: String,
    systemPrompt: String?,
  ): String {
    val client = getClient(projectId) // Reuse the client based on the projectId

    val params =
      ChatCompletionCreateParams
        .builder()
        .model(ChatModel.GPT_4O)
        .apply {
          systemPrompt?.let { addSystemMessage(it) }
          addUserMessage(userPrompt)
        }.build()

    return client
      .chat()
      .completions()
      .create(params)
      .choices()
      .firstOrNull()
      ?.message()
      ?.content()
      ?.orElse(null) ?: throw IllegalStateException("OpenAI response is missing content")
  }
}

@ConfigurationProperties("airbyte.openai.api-keys")
data class OpenAIProjectAPIKeyConfig(
  val failedSyncAssistant: String,
)

enum class OpenAIProjectId {
  FailedSyncAssistant,
}
