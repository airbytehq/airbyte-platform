/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

import java.util.UUID

/**
 * Anonymous UUID to be used with anonymous contexts.
 *
 * Annotated with @JvmField for java interop.
 */
@JvmField
val ANONYMOUS = UUID(0, 0)

/**
 * Context abstraction for feature flag evaluation, inspired by LaunchDarkly v6 context model.
 *
 * Contexts represent the entities for which feature flags are being evaluated. They provide
 * a way to target specific users, workspaces, organizations, or other entities with different
 * flag values based on their characteristics.
 *
 * The sealed interface design ensures type safety and consistency across the codebase,
 * preventing incorrect context usage and enabling compile-time validation.
 *
 * @property [kind] The type identifier for this context (e.g., "user", "workspace", "organization").
 *                 Must be consistent for each implementation and should not vary per instance.
 * @property [key] The unique identifier for the specific entity this context represents
 *                (e.g., user UUID, workspace UUID, organization UUID).
 * @property [attrs] Optional list of additional attributes that provide extra targeting capabilities
 *                  (e.g., email addresses, geographic regions, plan types).
 */
sealed interface Context {
  val kind: String
  val key: String
  val attrs: List<Attribute>
    get() = emptyList()
}

/**
 * Additional attributes that can be attached to contexts for enhanced targeting capabilities.
 *
 * Attributes allow feature flag systems to make more sophisticated targeting decisions
 * beyond just the primary context key. For example, a user context might include
 * email attributes for targeting specific domains, or geographic attributes for
 * regional feature rollouts.
 *
 * @property [key] The identifier for this attribute type (e.g., "email", "region", "plan")
 * @property [value] The actual value of the attribute as a string
 * @property [private] Whether this attribute contains sensitive or PII data that should
 *                    be handled with extra care by feature flag providers
 */
sealed interface Attribute {
  val key: String
  val value: String
  val private: Boolean
}

/**
 * Email context attribute
 *
 * @param [email] the email address
 */
data class EmailAttribute(
  val email: String,
) : Attribute {
  override val key = "email"
  override val value = email
  override val private = true
}

/**
 * A special context type that represents multiple contexts simultaneously.
 *
 * Multi contexts are useful when you need to evaluate feature flags based on multiple
 * entities at once. For example, you might want to target based on both the user
 * and the workspace they're operating in, or both the connection and the organization.
 *
 * This enables complex targeting scenarios like:
 * - Enable feature for specific users in specific workspaces
 * - Different behavior based on user + organization + geographic region
 * - Connection-specific features that also consider the workspace context
 *
 * Note: Multi contexts cannot be nested (a Multi cannot contain another Multi).
 *
 * @param [contexts] The set of individual contexts to evaluate simultaneously.
 *                  Must not be empty and must not contain other Multi contexts.
 */
data class Multi(
  val contexts: Set<Context>,
) : Context {
  constructor(contexts: List<Context>) : this(contexts = contexts.toSet())

  /** This value MUST be "multi" to properly sync with the LaunchDarkly client. */
  override val kind = "multi"

  /**
   * Multi contexts (in LDv6) do not have a key, default to an empty string.
   */
  override val key = ""

  init {
    if (contexts.isEmpty()) {
      throw IllegalArgumentException("Contexts cannot be empty")
    }
    // ensure there are no nested contexts (i.e. this Multi does not contain another Multi)
    if (fetchContexts<Multi>().isNotEmpty()) {
      throw IllegalArgumentException("Multi contexts cannot be nested")
    }
  }

  /**
   * Extracts all contexts of a specific type from this Multi context.
   *
   * This is useful when you need to access specific types of contexts within a Multi,
   * for example to get all Workspace contexts or all User contexts for specialized logic.
   *
   * @param T The specific Context type to extract (e.g., Workspace::class, User::class)
   * @return A list of all contexts matching type T, or empty list if none are found
   */
  internal inline fun <reified T> fetchContexts(): List<T> = contexts.filterIsInstance<T>()

  companion object {
    /**
     * Constructs a new [Multi] from the given contexts or returns an [Empty] if given contexts are empty.
     */
    fun orEmpty(contexts: List<Context>): Context = if (contexts.isEmpty()) Empty else Multi(contexts)
  }
}

/**
 * Context for representing an organization.
 *
 * @param [key] the unique identifying value of this organization
 */
data class Organization(
  override val key: String,
) : Context {
  override val kind = "organization"

  /**
   * Secondary constructor
   *
   * @param [key] organization UUID
   */
  constructor(key: UUID) : this(key = key.toString())
}

/**
 * Context for representing a workspace.
 *
 * @param [key] the unique identifying value of this workspace
 */
data class Workspace(
  override val key: String,
) : Context {
  override val kind = "workspace"

  /**
   * Secondary constructor
   *
   * @param [key] workspace UUID
   */
  constructor(key: UUID) : this(key = key.toString())
}

/**
 * Context for representing a user.
 *
 * @param [key] the unique identifying value of this user
 */
data class User(
  override val key: String,
  override val attrs: List<Attribute> = emptyList(),
) : Context {
  override val kind = "user"

  /**
   * Secondary constructor
   *
   * @param [key] user UUID
   */
  constructor(key: UUID) : this(key = key.toString(), attrs = emptyList())

  /**
   * User constructor with email attribute
   *
   * @param [key] user UUID
   * @param [email] user email attribute
   */
  constructor(key: UUID, email: EmailAttribute) : this(key = key.toString(), attrs = listOf(email))
}

/**
 * Context for representing a connection.
 *
 * @param [key] the unique identifying value of this connection
 */
data class Connection(
  override val key: String,
) : Context {
  override val kind = "connection"

  /**
   * Secondary constructor
   *
   * @param [key] connection UUID
   */
  constructor(key: UUID) : this(key = key.toString())
}

/**
 * Context for representing a source actor.
 *
 * @param [key] the unique identifying value of this source
 */
data class Source(
  override val key: String,
) : Context {
  override val kind = "source"

  /**
   * Secondary constructor
   *
   * @param [key] Source Actor UUID
   */
  constructor(key: UUID) : this(key = key.toString())
}

/**
 * Context for representing a destination actor.
 *
 * @param [key] the unique identifying value of this destination
 */
data class Destination(
  override val key: String,
) : Context {
  override val kind = "destination"

  /**
   * Secondary constructor
   *
   * @param [key] Destination Actor UUID
   */
  constructor(key: UUID) : this(key = key.toString())
}

/**
 * Context for representing a source definition.
 *
 * @param [key] the unique identifying value of this source definition
 */
data class SourceDefinition(
  override val key: String,
) : Context {
  override val kind = "source-definition"

  /**
   * Secondary constructor
   *
   * @param [key] SourceDefinition UUID
   */
  constructor(key: UUID) : this(key = key.toString())
}

/**
 * Context for representing a destination definition.
 *
 * @param [key] the unique identifying value of this destination definition
 */
data class DestinationDefinition(
  override val key: String,
) : Context {
  override val kind = "destination-definition"

  /**
   * Secondary constructor
   *
   * @param [key] DestinationDefinition UUID
   */
  constructor(key: UUID) : this(key = key.toString())
}

/**
 * Context for representing a source type.
 *
 * @param [key] the type of source
 */
data class SourceType(
  override val key: String,
) : Context {
  override val kind = "source-type"
}

data class ImageName(
  override val key: String,
) : Context {
  override val kind = "image-name"
}

data class ImageVersion(
  override val key: String,
) : Context {
  override val kind = "image-version"
}

/**
 * Context for representing a plane by name.
 *
 * For example: prod-gcp-dataplane-us-west-1-1
 *
 * @param [key] the name of the plane.
 */
data class PlaneName(
  override val key: String,
) : Context {
  override val kind: String = "plane-name"
}

data class Priority(
  override val key: String,
) : Context {
  override val kind: String = "priority"

  companion object {
    const val HIGH_PRIORITY = "high"
  }
}

/**
 * Context for representing an attempt number.
 *
 * For example: 0
 *
 * @param [key] the number of the attempt.
 */
data class Attempt(
  override val key: String,
) : Context {
  override val kind = "attempt"
}

data class UserAgent(
  override val key: String,
) : Context {
  override val kind: String = "user-agent"
}

data class RequestId(
  override val key: String,
) : Context {
  override val kind: String = "request-id"

  // secondary constructor
  constructor(key: UUID) : this(key = key.toString())
}

// This is aimed to be used with the EnvFeatureFlag
data object Empty : Context {
  override val kind: String = "empty"

  // key needs to be not null or empty for LD to accept the context
  override val key: String = "empty"
}

data class CloudProvider(
  override val key: String,
) : Context {
  override val kind: String = "cloud-provider"

  companion object {
    const val AWS = "aws"
  }
}

data class GeographicRegion(
  override val key: String,
) : Context {
  override val kind: String = "geographic-region"

  companion object {
    const val US = "us"
    const val EU = "eu"
  }
}

/**
 * Context for representing a dataplane.
 *
 * @param [key] the unique identifying value of this dataplane.
 */
data class Dataplane(
  override val key: String,
) : Context {
  override val kind: String = "dataplane"

  /**
   * Secondary constructor
   *
   * @param [key] dataplane UUID
   */
  constructor(key: UUID) : this(key = key.toString())
}

/**
 * Context for representing a dataplane group.
 *
 * @param [key] the unique identifying value of this dataplane group.
 */
data class DataplaneGroup(
  override val key: String,
) : Context {
  override val kind: String = "dataplane-group"

  /**
   * Secondary constructor
   *
   * @param [key] dataplane group UUID
   */
  constructor(key: UUID) : this(key = key.toString())
}

data class CloudProviderRegion(
  override val key: String,
) : Context {
  override val kind: String = "cloud-provider-region"

  companion object {
    const val AWS_US_EAST_1 = "us-east-1"
  }
}

/**
 * Context for representing the JobType.
 */
data class JobType(
  override val key: String,
) : Context {
  override val kind: String = "job-type"
}

data class SecretStorage(
  override val key: String,
) : Context {
  override val kind: String = "secret-storage"
}

data class TokenSubject(
  override val key: String,
) : Context {
  override val kind: String = "token-subject"
}

/**
 * Merges two contexts into a single context for combined feature flag evaluation.
 *
 * This extension function provides a convenient way to combine contexts when you need
 * to evaluate flags based on multiple entities. The merge logic handles various scenarios:
 * - If contexts are identical, returns the original
 * - Empty contexts are ignored
 * - Multi contexts are combined efficiently
 * - Single contexts are converted to Multi when merged
 *
 * @param other The context to merge with this context
 * @return A combined context representing both original contexts
 */
fun Context.merge(other: Context): Context =
  when {
    this == other -> this
    other is Empty -> this
    this is Empty -> other
    this is Multi && other is Multi -> Multi(this.contexts + other.contexts)
    this is Multi -> Multi(this.contexts + other)
    other is Multi -> Multi(other.contexts + this)
    else -> Multi(listOf(this, other))
  }
