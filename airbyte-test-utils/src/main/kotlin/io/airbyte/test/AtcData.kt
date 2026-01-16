/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Defines the data for the acceptance test connector.
 */
interface AtcData {
  /**
   * The cursor(s) (primary key(s) of the data used by this connector.
   */
  fun cursor(): List<String>

  /**
   * The list of required fields for this connector.
   */
  fun required(): List<String>

  /**
   * A map of field-name to type for this connector.
   *
   * ```kotlin
   * mapOf("id" to "number", "firstName" to "string")
   * ```
   */
  fun properties(): Map<String, AtcDataProperty>

  /**
   * Records this connector will send (if source) or should receive (if destination).
   */
  fun records(): List<Any>
}

/**
 * Represents a property definition for acceptance test connector data schemas.
 * Used to define the structure and characteristics of fields in test data.
 *
 * @param type The data type of the property (e.g., "string", "number", "boolean").
 * @param secret Whether this property should be treated as a secret field in Airbyte.
 *               When true, the field will be serialized with the "airbyte_secret" annotation
 *               and excluded from non-default JSON serialization for security purposes.
 */
data class AtcDataProperty(
  val type: String,
  @get:JsonProperty("airbyte_secret")
  @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
  val secret: Boolean = false,
)

/**
 * Movie data for usage in acceptance tests
 */
object AtcDataMovies : AtcData {
  override fun cursor() = listOf("film")

  override fun required() = listOf("year", "film", "publisher", "director", "distributor")

  override fun properties() =
    mapOf(
      "year" to AtcDataProperty(type = "number"),
      "film" to AtcDataProperty(type = "string"),
      "publisher" to AtcDataProperty(type = "string"),
      "director" to AtcDataProperty(type = "string"),
      "distributor" to AtcDataProperty(type = "string"),
      "worldwide_gross" to AtcDataProperty(type = "string"),
    )

  override fun records(): List<Any> =
    listOf(
      Movie(
        year = 2020,
        film = "Sonic the Hedgehog",
        publisher = "Sega Sammy Group",
        director = "Jeff Fowler",
        distributor = "Paramount Pictures",
        worldwideGross = "$320,954,026",
      ),
      Movie(
        year = 2022,
        film = "Sonic the Hedgehog 2",
        publisher = "Sega Sammy Group",
        director = "Jeff Fowler",
        distributor = "Paramount Pictures",
        worldwideGross = "$405,421,518",
      ),
      Movie(
        year = 2024,
        film = "Sonic the Hedgehog 3",
        publisher = "Sega Sammy Group",
        director = "Jeff Fowler",
        distributor = "Paramount Pictures",
        worldwideGross = "$491,603,986",
      ),
    )
}

@JsonInclude(JsonInclude.Include.NON_EMPTY)
data class Movie(
  val year: Int,
  val film: String,
  val publisher: String,
  val director: String,
  val distributor: String,
  @field:JsonProperty("worldwide_gross")
  val worldwideGross: String,
  val headliner: String? = null,
)
