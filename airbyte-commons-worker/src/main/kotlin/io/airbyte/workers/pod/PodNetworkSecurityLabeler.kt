package io.airbyte.workers.pod

import io.airbyte.workers.hashing.Hasher
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy
import io.fabric8.kubernetes.client.KubernetesClient
import io.micronaut.cache.CacheManager
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Class which lets us get the network security labels for pods.
 * Tokens will be on the networkPolicies themselves in the format:
 * airbyte/networkSecurityTokenHash: sha256(token, salt)
 *
 * These will be cached to avoid hitting the kube API every  single time (should change infrequently)
 */
@Singleton
class PodNetworkSecurityLabeler(
  private val networkPolicyFetcher: NetworkPolicyFetcher?,
  cacheManager: CacheManager<Any>,
  private val hasher: Hasher,
) {
  private val cache = cacheManager.getCache("network-security-labels")

  fun getLabels(
    workspaceId: UUID?,
    networkSecurityTokens: List<String>,
  ): Map<String, String> {
    return workspaceId?.let {
      networkPolicyFetcher?.let {
        if (networkSecurityTokens.isEmpty()) {
          // Short circuit if we have no tokens to fetch policies for
          return emptyMap()
        }
        try {
          val cachedLabels = cache.get(workspaceId, Map::class.java)
          if (cachedLabels.isPresent && cachedLabels.get().isNotEmpty()) {
            return cachedLabels.get() as Map<String, String>
          }
          val matchingNetworkPolicies = networkPolicyFetcher.matchingNetworkPolicies(workspaceId, networkSecurityTokens, hasher)

          val labels = flatten(matchingNetworkPolicies.map { it.spec.podSelector.matchLabels })
          cache.put(workspaceId, labels)
          return labels
        } catch (e: Exception) {
          logger.error(e) { "Failed to get network security labels for workspace $workspaceId" }
          return emptyMap()
        }
      } ?: run {
        logger.debug { "NetworkPolicyFetcher is null, skipping network security labels" }
        emptyMap()
      }
    } ?: run {
      logger.debug { "Workspace ID is null, skipping network security labels" }
      emptyMap()
    }
  }

  private fun <K, V> flatten(list: List<Map<K, V>>): Map<K, V> =
    mutableMapOf<K, V>().apply {
      for (innerMap in list) putAll(innerMap)
    }
}

@Singleton
@Requires(property = "airbyte.workload-launcher.network-policy-introspection", value = "true")
class NetworkPolicyFetcher(
  private val kubernetesClient: KubernetesClient,
) {
  private val tokenHashKey = "airbyte/networkSecurityTokenHash"
  private val salt = "airbyte.network.security.token"
  private val workspaceIdLabelKey = "airbyte/workspaceId"

  fun matchingNetworkPolicies(
    workspaceId: UUID,
    networkSecurityTokens: List<String>,
    hasher: Hasher,
  ): List<NetworkPolicy> {
    // Need to truncate the token because labels can only be so long
    val hashedTokens = networkSecurityTokens.map { hasher.hash(it, salt).slice(IntRange(0, 49)) }
    return kubernetesClient
      .network()
      .networkPolicies()
      .inNamespace("jobs")
      .withLabelIn(tokenHashKey, *hashedTokens.toTypedArray())
      .withLabel(workspaceIdLabelKey, workspaceId.toString())
      .list()
      .items
  }
}
