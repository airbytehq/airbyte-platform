package io.airbyte.featureflag.server

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.airbyte.featureflag.server.model.Context
import io.airbyte.featureflag.server.model.FeatureFlag
import io.airbyte.featureflag.server.model.Rule
import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.isRegularFile

// This is open for testing, creating an interface might be the way to go
@Singleton
open class FeatureFlagService(
  @Property(name = "airbyte.feature-flag.path") configPath: Path?,
) {
  private val flags = mutableMapOf<String, MutableFeatureFlag>()

  init {
    configPath?.also { path ->
      if (path.exists() && path.isRegularFile()) {
        val yamlMapper = ObjectMapper(YAMLFactory()).registerKotlinModule()
        val config = yamlMapper.readValue(path.toFile(), ConfigFileFlags::class.java)
        config.flags.forEach {
          put(it.toFeatureFlag())
        }
      }
    }
  }

  open fun delete(key: String) {
    flags.remove(key)
  }

  open fun eval(
    key: String,
    context: Map<String, String>,
  ): String? {
    val flag = flags[key] ?: return null
    for (rule in flag.rules) {
      if (rule.context.matches(context)) {
        return rule.value
      }
    }
    return flag.default
  }

  open fun get(key: String): FeatureFlag? {
    return flags[key]?.toFeatureFlag()
  }

  open fun addRule(
    key: String,
    rule: Rule,
  ): FeatureFlag {
    val flag = flags[key] ?: throw Exception("$key not found")

    if (flag.rules.any { it.context == rule.context }) {
      throw Exception("$key already has a rule for context ${rule.context}")
    }
    flag.rules.add(rule.toMutableRule())
    return flag.toFeatureFlag()
  }

  open fun updateRule(
    key: String,
    rule: Rule,
  ): FeatureFlag {
    val flag = flags[key] ?: throw Exception("$key not found")
    flag.rules
      .find { it.context == rule.context }
      ?.apply { value = rule.value }
      ?: throw Exception("$key does not have a rule for context ${rule.context}")
    return flag.toFeatureFlag()
  }

  open fun removeRule(
    key: String,
    context: Context,
  ): FeatureFlag {
    val flag = flags[key] ?: throw Exception("$key not found")
    flag.rules.removeIf { it.context == context }
    return flag.toFeatureFlag()
  }

  open fun put(flag: FeatureFlag): FeatureFlag {
    flags[flag.key] = flag.toMutableFeatureFlag()
    return get(flag.key) ?: throw Exception("Failed to put flag $flag")
  }

  open fun put(
    key: String,
    default: String,
    rules: List<Rule> = emptyList(),
  ): FeatureFlag {
    val flag = FeatureFlag(key = key, default = default, rules = rules)
    return put(flag)
  }

  private fun Context.matches(env: Map<String, String>): Boolean = env[kind] == value

  private fun MutableFeatureFlag.toFeatureFlag(): FeatureFlag = FeatureFlag(key = key, default = default, rules = rules.map { it.toRule() }.toList())

  private fun FeatureFlag.toMutableFeatureFlag(): MutableFeatureFlag =
    MutableFeatureFlag(key = key, default = default, rules = rules.map { it.toMutableRule() }.toMutableList())

  private fun MutableRule.toRule(): Rule = Rule(context = context, value = value)

  private fun Rule.toMutableRule(): MutableRule = MutableRule(context = context, value = value)

  private data class MutableFeatureFlag(val key: String, val default: String, val rules: MutableList<MutableRule>)

  private data class MutableRule(val context: Context, var value: String)

  private data class ConfigFileFlags(val flags: List<ConfigFileFlag>)

  private data class ConfigFileFlag(
    val name: String,
    val serve: String,
    val context: List<ConfigFileFlagContext>? = null,
  ) {
    fun toFeatureFlag(): FeatureFlag {
      val rules = context?.flatMap { context -> context.include.map { Rule(Context(kind = context.type, value = it), value = context.serve) } }
      return FeatureFlag(key = name, default = serve, rules = rules ?: emptyList())
    }
  }

  private data class ConfigFileFlagContext(
    val type: String,
    val serve: String,
    val include: List<String> = listOf(),
  )
}
