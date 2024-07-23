package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.NotificationItem
import io.airbyte.api.model.generated.NotificationSettings
import io.airbyte.api.model.generated.NotificationType
import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.server.converters.NotificationConverter
import io.airbyte.commons.server.converters.NotificationSettingsConverter
import io.airbyte.commons.server.converters.WorkspaceWebhookConfigsConverter
import io.airbyte.config.Geography
import io.airbyte.config.Organization
import io.airbyte.config.StandardWorkspace
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import kotlin.jvm.optionals.getOrNull

// These helpers exist so that we can get some of the utility of working with workspaces but without needing to inject WorkspacesHandler

fun buildStandardWorkspace(
  workspaceCreateWithId: WorkspaceCreateWithId,
  organization: Organization,
  uuidSupplier: Supplier<UUID>,
): StandardWorkspace {
  // if not set on the workspaceCreate, set the defaultGeography to AUTO
  val geography: Geography = workspaceCreateWithId.defaultGeography?.let { Enums.convertTo(it, Geography::class.java) } ?: Geography.AUTO

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
    notificationSettings = NotificationSettingsConverter.toConfig(patchNotificationSettingsWithDefaultValue(workspaceCreateWithId))
    defaultGeography = geography
    webhookOperationConfigs = WorkspaceWebhookConfigsConverter.toPersistenceWrite(workspaceCreateWithId.webhookConfigs, uuidSupplier)
    organizationId = organization.organizationId
    email = workspaceCreateWithId.email
  }
}

private fun patchNotificationSettingsWithDefaultValue(workspaceCreateWithId: WorkspaceCreateWithId): NotificationSettings {
  val defaultNotificationType = NotificationItem().addNotificationTypeItem(NotificationType.CUSTOMERIO)

  return NotificationSettings().apply {
    // Grab a reference to this `apply` scope's `this`, could also avoid this and reference `this@apply` in the following `with` function instead.
    val ns = this

    with(workspaceCreateWithId.notificationSettings) {
      ns.sendOnSuccess = this?.sendOnSuccess ?: NotificationItem().notificationType(emptyList())
      ns.sendOnFailure = this?.sendOnFailure ?: defaultNotificationType
      ns.sendOnConnectionUpdate = this?.sendOnConnectionUpdate ?: defaultNotificationType

      ns.sendOnConnectionUpdateActionRequired = this?.sendOnConnectionUpdateActionRequired ?: defaultNotificationType

      ns.sendOnSyncDisabled = this?.sendOnSyncDisabled ?: defaultNotificationType
      ns.sendOnSyncDisabledWarning = this?.sendOnSyncDisabledWarning ?: defaultNotificationType
      ns.sendOnBreakingChangeWarning = this?.sendOnBreakingChangeWarning ?: defaultNotificationType

      ns.sendOnBreakingChangeSyncsDisabled = this?.sendOnBreakingChangeSyncsDisabled ?: defaultNotificationType
    }
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
