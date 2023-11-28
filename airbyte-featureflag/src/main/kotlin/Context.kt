/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
 */
sealed interface Context {
  val kind: String
  val key: String
}

/**
 * Context for representing multiple contexts concurrently.  Only supported for LaunchDarkly v6!
 *
 *  @param [contexts] list of contexts, must not contain another Multi context
 */
data class Multi(val contexts: List<Context>) : Context {
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
  internal inline fun <reified T> fetchContexts(): List<T> {
    return contexts.filterIsInstance<T>()
  }
}

/**
 * Context for representing an organization.
 *
 * @param [key] the unique identifying value of this organization
 */
data class Organization constructor(override val key: String) : Context {
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
data class Workspace constructor(override val key: String) : Context {
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
data class User(override val key: String) : Context {
  override val kind = "user"

  /**
   * Secondary constructor
   *
   * @param [key] user UUID
   */
  constructor(key: UUID) : this(key = key.toString())
}

/**
 * Context for representing a connection.
 *
 * @param [key] the unique identifying value of this connection
 */
data class Connection(override val key: String) : Context {
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
data class Source(override val key: String) : Context {
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
data class Destination(override val key: String) : Context {
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
data class SourceDefinition(override val key: String) : Context {
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
data class DestinationDefinition(override val key: String) : Context {
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
data class SourceType(override val key: String) : Context {
  override val kind = "source-type"
}

data class ImageName(override val key: String) : Context {
  override val kind = "image-name"
}

data class ImageVersion(override val key: String) : Context {
  override val kind = "image-version"
}

data class Geography(override val key: String) : Context {
  override val kind: String = "geography"
}
