/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.dsr

import java.util.UUID

/**
 * Deletes an organization's audit log documents from object storage.
 *
 * Deletion is organization-scoped: every file partitioned under the organization is removed. On
 * the DSR path this means audit logs are only deleted when the data subject is the OWNER of the
 * organization — entries where they appear as an actor in orgs they merely belonged to are not
 * covered, because those tenants are not part of the deletion manifest.
 *
 * Implemented by the audit-logging module. The interface lives in this module (rather than next to
 * the implementation) because the DSR deletion flow runs here, and this module cannot depend on
 * `airbyte-audit-logging` — that module already depends on this one, so a reverse dependency would
 * be circular.
 */
interface DsrAuditLogDeletion {
  /**
   * Hard-deletes every stored audit log file partitioned under the organization.
   *
   * @return the number of audit log files deleted.
   */
  fun deleteAuditLogsByOrganizationId(organizationId: UUID): Int
}
