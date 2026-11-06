import { DataplaneGroupRead } from "core/api/types/AirbyteClient";

// The groups list holds the organization's own regions plus shared ones owned by Airbyte
// (the default organization and, with the entitlement, the PrivateLink organization).
export const isAirbyteManagedRegion = (
  group: Pick<DataplaneGroupRead, "organization_id">,
  organizationId: string
): boolean => group.organization_id !== organizationId;
