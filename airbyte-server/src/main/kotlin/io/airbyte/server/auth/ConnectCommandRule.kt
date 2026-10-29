/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import com.google.common.net.InternetDomainName
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.RunConnectCommandRequest
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.net.parseIpLiteral
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

private const val MAX_PORT = 65535

data class ConnectCommandInput(
  val workspaceId: UUID,
) : OperationInput

@Singleton
class ConnectCommandRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): ConnectCommandInput {
    val body = args.singleOrNull() as? RunConnectCommandRequest ?: throw BadRequestProblem()
    if (body.id.isBlank()) throw BadRequestProblem()
    if (!isValidHost(body.host)) throw BadRequestProblem()
    if (body.port !in 1..MAX_PORT) throw BadRequestProblem()
    return ConnectCommandInput(body.workspaceId)
  }

  // An IP literal or a DNS hostname. Anything else (blank, a URL, spaces, a malformed IP like 999.0.0.1) is bad input.
  private fun isValidHost(host: String): Boolean = parseIpLiteral(host) != null || InternetDomainName.isValid(host)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as ConnectCommandInput
    val owner = data.workspaceAndOrganization(request.workspaceId)
    permissions.requireWorkspaces(
      caller,
      listOf(owner),
      setOf(AuthRoleConstants.WORKSPACE_RUNNER, AuthRoleConstants.EMBEDDED_END_USER),
      OperationCaller.entries.toSet(),
    )
  }
}
