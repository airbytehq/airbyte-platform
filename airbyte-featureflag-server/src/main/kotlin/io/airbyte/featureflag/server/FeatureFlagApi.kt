/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag.server

import io.airbyte.featureflag.server.model.Context
import io.airbyte.featureflag.server.model.FeatureFlag
import io.airbyte.featureflag.server.model.Rule
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Consumes
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.QueryValue
import io.micronaut.scheduling.TaskExecutors
import io.micronaut.scheduling.annotation.ExecuteOn
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.parameters.RequestBody
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import jakarta.ws.rs.DELETE
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.PUT
import jakarta.ws.rs.Path

@Controller("/api/v1/feature-flags")
@ExecuteOn(TaskExecutors.IO)
class FeatureFlagApi(
  private val ffs: FeatureFlagService,
) {
  @DELETE
  @Path("/{key}")
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "The flag evaluation",
      ),
    ],
  )
  fun delete(
    @PathVariable key: String,
  ) {
    ffs.delete(key)
  }

  @GET
  @Path("/{key}")
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "The FeatureFlag",
        content = [Content(schema = Schema(implementation = FeatureFlag::class))],
      ),
    ],
  )
  fun get(
    @PathVariable key: String,
  ): FeatureFlag = ffs.get(key) ?: throw KnownException(HttpStatus.NOT_FOUND, "$key not found")

  @PUT
  @Path("/")
  @Consumes("application/json")
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "The FeatureFlag",
        content = [Content(schema = Schema(implementation = FeatureFlag::class))],
      ),
    ],
  )
  fun put(
    @RequestBody(content = [Content(schema = Schema(implementation = FeatureFlag::class))]) @Body request: FeatureFlag,
  ): FeatureFlag = ffs.put(request)

  @GET
  @Path("/{key}/evaluate")
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "The flag evaluation",
      ),
    ],
  )
  fun evaluate(
    @PathVariable key: String,
    @QueryValue("kind") kind: List<String> = emptyList(),
    @QueryValue("value") value: List<String> = emptyList(),
  ): String {
    val context = kind.zip(value).toMap()
    return ffs.eval(key, context) ?: throw KnownException(HttpStatus.NOT_FOUND, "$key not found")
  }

  @DELETE
  @Path("/{key}/rules")
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "The flag evaluation",
      ),
    ],
  )
  fun deleteRule(
    @PathVariable key: String,
    @RequestBody(content = [Content(schema = Schema(implementation = Context::class))]) @Body context: Context,
  ): FeatureFlag = ffs.removeRule(key, context)

  @POST
  @Path("/{key}/rules")
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "The flag evaluation",
      ),
    ],
  )
  fun postRule(
    @PathVariable key: String,
    @RequestBody(content = [Content(schema = Schema(implementation = Rule::class))]) @Body rule: Rule,
  ): FeatureFlag = ffs.addRule(key, rule)

  @PUT
  @Path("/{key}/rules")
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "The flag evaluation",
      ),
    ],
  )
  fun putRule(
    @PathVariable key: String,
    @RequestBody(content = [Content(schema = Schema(implementation = Rule::class))]) @Body rule: Rule,
  ): FeatureFlag = ffs.updateRule(key, rule)
}

class KnownException(
  val httpCode: HttpStatus,
  message: String,
) : Throwable(message)
