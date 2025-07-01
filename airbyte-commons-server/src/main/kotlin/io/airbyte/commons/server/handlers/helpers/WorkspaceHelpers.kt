/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.google.common.base.Strings
import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.NotificationMissingUrlProblem
import io.airbyte.api.problems.throwable.generated.NotificationRequiredProblem
import io.airbyte.commons.server.converters.NotificationConverter
import io.airbyte.commons.server.converters.NotificationSettingsConverter
import io.airbyte.commons.server.converters.WorkspaceWebhookConfigsConverter
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.Notification
import io.airbyte.config.Organization
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.helpers.patchNotificationSettingsWithDefaultValue
import io.airbyte.data.services.DataplaneGroupService
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import kotlin.jvm.optionals.getOrNull

// These helpers exist so that we can get some of the utility of working with workspaces but without needing to inject WorkspacesHandler

fun buildStandardWorkspace(
  workspaceCreateWithId: WorkspaceCreateWithId,
  organization: Organization,
  uuidSupplier: Supplier<UUID>,
  dataplaneGroupService: DataplaneGroupService,
  airbyteEdition: AirbyteEdition,
): StandardWorkspace {
  // if not set on the workspaceCreate, set the dataplaneGroupId to the default
  val resolvedDataplaneGroupId =
    workspaceCreateWithId.dataplaneGroupId ?: dataplaneGroupService.getDefaultDataplaneGroupForAirbyteEdition(airbyteEdition).id

  return StandardWorkspace().apply {
    workspaceId = workspaceCreateWithId.id ?: uuidSupplier.get()
    customerId = uuidSupplier.get() // "customer_id" should be deprecated
    name = workspaceCreateWithId.name
    slug = uuidSupplier.get().toString()
    initialSetupComplete = false
    anonymousDataCollection = workspaceCreateWithId.anonymousDataCollection ?: false
    news = workspaceCreateWithId.news ?: false
    securityUpdates = workspaceCreateWithId.securityUpdates ?: false
    displaySetupWizard = workspaceCreateWithId.displaySetupWizard ?: false
    tombstone = false
    notifications = NotificationConverter.toConfigList(workspaceCreateWithId.notifications)
    notificationSettings =
      patchNotificationSettingsWithDefaultValue(NotificationSettingsConverter.toConfig(workspaceCreateWithId.notificationSettings))
    dataplaneGroupId = resolvedDataplaneGroupId
    webhookOperationConfigs = WorkspaceWebhookConfigsConverter.toPersistenceWrite(workspaceCreateWithId.webhookConfigs, uuidSupplier)
    organizationId = organization.organizationId
    email = workspaceCreateWithId.email
  }
}

fun getDefaultWorkspaceName(
  organization: Optional<Organization>,
  companyName: String?,
  email: String,
): String {
  // use organization name as default workspace name, if present
  var defaultWorkspaceName: String = organization.getOrNull()?.name?.trim() ?: ""

  // if organization name is not available or empty, use user's company name (note: this is an optional field)
  if (defaultWorkspaceName.isEmpty() && companyName != null) {
    defaultWorkspaceName = companyName.trim()
  }
  // if company name is still empty, use user's email (note: this is a required field)
  if (defaultWorkspaceName.isEmpty()) {
    defaultWorkspaceName = email
  }

  return defaultWorkspaceName
}

fun validateWorkspace(
  workspace: StandardWorkspace,
  airbyteEdition: AirbyteEdition,
) {
  if (workspace.notificationSettings != null) {
    val settings = workspace.notificationSettings
    validateNotificationItem(settings.sendOnSuccess, "success")
    validateNotificationItem(settings.sendOnFailure, "failure")
    validateNotificationItem(settings.sendOnConnectionUpdate, "connectionUpdate")
    validateNotificationItem(settings.sendOnConnectionUpdateActionRequired, "connectionUpdateActionRequired")
    validateNotificationItem(settings.sendOnSyncDisabled, "syncDisabled")
    validateNotificationItem(settings.sendOnSyncDisabledWarning, "syncDisabledWarning")

    // email notifications for connectionUpdateActionRequired and syncDisabled can't be disabled.
    // this rule only applies to Airbyte Cloud, because OSS doesn't support email notifications.
    if (airbyteEdition == AirbyteEdition.CLOUD) {
      if (!settings.sendOnConnectionUpdateActionRequired.hasEmail()) {
        throw NotificationRequiredProblem(
          ProblemMessageData().message("The 'connectionUpdateActionRequired' email notification can't be disabled"),
        )
      }
      if (!settings.sendOnSyncDisabled.hasEmail()) {
        throw NotificationRequiredProblem(
          ProblemMessageData().message("The 'syncDisabled' email notification can't be disabled"),
        )
      }
    }
  }
}

private fun io.airbyte.config.NotificationItem?.hasEmail(): Boolean {
  if (this == null) return false
  return this.notificationType.contains(Notification.NotificationType.CUSTOMERIO)
}

private fun validateNotificationItem(
  item: io.airbyte.config.NotificationItem?,
  notificationName: String,
) {
  if (item == null) {
    return
  }

  if (item.notificationType != null && item.notificationType.contains(Notification.NotificationType.SLACK)) {
    if (item.slackConfiguration == null || Strings.isNullOrEmpty(item.slackConfiguration.webhook)) {
      throw NotificationMissingUrlProblem(
        ProblemMessageData().message(String.format("The '%s' notification is enabled but is missing a URL.", notificationName)),
      )
    }
  }
}
