/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import io.airbyte.workers.hashing.TestHasher
import io.fabric8.kubernetes.api.model.LabelSelector
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicySpec
import io.micronaut.cache.CacheManager
import io.micronaut.cache.SyncCache
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

internal class PodNetworkSecurityLabelerTest {
  private lateinit var mNetworkPolicyFetcher: NetworkPolicyFetcher
  private lateinit var mCacheManager: CacheManager<Any>
  private lateinit var mCache: SyncCache<Any>
  private val tokenHashKey = "airbyte/networkSecurityTokenHash"

  @BeforeEach
  fun setup() {
    mNetworkPolicyFetcher = mockk()
    mCacheManager = mockk()
    mCache = mockk()

    every { mCacheManager.getCache(any()) } returns mCache
  }

  @Test
  fun testGetLabels() {
    val labeler = PodNetworkSecurityLabeler(mNetworkPolicyFetcher, mCacheManager, TestHasher())
    val networkSecurityTokens = listOf("token1", "token2")
    val workspaceId = UUID.randomUUID()
    // No cache
    every { mCache.get(workspaceId, Map::class.java) } returns Optional.empty()
    every { mCache.put(any(), any()) } just runs
    every { mNetworkPolicyFetcher.matchingNetworkPolicies(workspaceId, networkSecurityTokens, any()) } returns
      listOf(
        NetworkPolicy().apply {
          spec =
            NetworkPolicySpec().apply {
              podSelector =
                LabelSelector().apply {
                  matchLabels = mapOf("Label1" to "value1")
                }
            }
        },
        NetworkPolicy().apply {
          spec =
            NetworkPolicySpec().apply {
              podSelector =
                LabelSelector().apply {
                  matchLabels = mapOf("Label2" to "value2")
                }
            }
        },
      )
    val result = labeler.getLabels(workspaceId, networkSecurityTokens)
    Assertions.assertEquals(mapOf("Label1" to "value1", "Label2" to "value2"), result)
  }

  @Test
  fun testShortCircuit() {
    val labeler = PodNetworkSecurityLabeler(mNetworkPolicyFetcher, mCacheManager, TestHasher())
    val networkSecurityTokens = emptyList<String>()
    val workspaceId = UUID.randomUUID()
    // No cache
    every { mCache.get(workspaceId, Map::class.java) } returns Optional.empty()
    every { mCache.put(any(), any()) } just runs
    val result = labeler.getLabels(workspaceId, networkSecurityTokens)
    Assertions.assertEquals(emptyMap<String, String>(), result)
    verify(exactly = 0) { mNetworkPolicyFetcher.matchingNetworkPolicies(any(), any(), any()) }
    verify(exactly = 0) { mCache.get(any(), Map::class.java) }
    verify(exactly = 0) { mCache.put(any(), any()) }
  }
}
