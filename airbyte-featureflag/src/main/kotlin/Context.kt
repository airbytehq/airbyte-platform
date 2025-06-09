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
 * Context abstraction around LaunchDarkly v6 context idea
 *
 * I'm still playing around with this.  Basically the idea is to define our own custom context types
 * (by implementing this sealed interface) to ensure that we are consistently using the same identifiers
 * throughout the code.
 *
 * @property [kind] determines the kind of context the implementation is,
 * must be consistent for each type and should not be set by the caller of a context
 * @property [key] is the unique identifier for the specific context, e.g. a user-id or workspace-id
 * @property [attrs] is a list of attributes that can be optionally added to a context
 */
sealed interface Context {
  val kind: String
  val key: String
  val attrs: List<Attribute>
    get() = emptyList()
}

/**
 * Additional context attributes that can be added to a context.
 *
 * @property [key] the key of the attribute
 * @property [value] the value of the attribute
 * @property [private] whether the attribute contains sensitive or PII data
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
 * Context for representing multiple contexts concurrently.  Only supported for LaunchDarkly v6!
 *
 *  @param [contexts] list of contexts, must not contain another Multi context
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
   * Returns all the [Context] types contained within this [Multi] matching type [T].
   *
   * @param [T] the [Context] type to fetch.
   * @return all [Context] of [T] within this [Multi], or an empty list if none match.
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

data class DataplaneGroup(
  override val key: String,
) : Context {
  override val kind: String = "dataplane-group"
}

data class CloudProviderRegion(
  override val key: String,
) : Context {
  override val kind: String = "cloud-provider-region"

  companion object {
    const val AWS_US_EAST_1 = "us-east-1"
  }
}

data class SecretStorage(
  override val key: String,
) : Context {
  override val kind: String = "secret-storage"
}

/**
 * Combines two contexts.
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
