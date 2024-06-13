package io.airbyte.workers.general

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Flag
import jakarta.inject.Singleton

/*
* FF client that ensures the value returned by first call is the value seen by subsequent calls.
* For use in the orchestrator, where we don't want values changing mid-sync and the cache will be
* torn down at the end.
*
* Don't abuse this.
*/
@Singleton
class CachingFeatureFlagClient(
  private val rawClient: FeatureFlagClient,
) {
  private val cache: LoadingCache<Pair<Flag<Boolean>, Context>, Boolean> =
    Caffeine
      .newBuilder()
      .build { key: Pair<Flag<Boolean>, Context> -> rawClient.boolVariation(key.first, key.second) }

  fun boolVariation(
    flag: Flag<Boolean>,
    ctx: Context,
  ): Boolean {
    return cache.get(Pair(flag, ctx))
  }
}
