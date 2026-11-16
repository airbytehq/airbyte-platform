import {
  DataplaneDiagnosticKind,
  DataplaneDiagnosticRead,
  DataplaneDiagnosticStatus,
  DataplaneHealthRead,
} from "core/api/types/AirbyteClient";

// The fields every data plane has. A test sets the rest, such as the last check-in, in its own overrides.
export const mockDataplane = (overrides: Partial<DataplaneHealthRead> = {}): DataplaneHealthRead => ({
  dataplane_id: "dp-1",
  dataplane_name: "us-east-1-flex-lab",
  dataplane_group_id: "group-1",
  organization_id: "org-1",
  status: "HEALTHY",
  ...overrides,
});

export const mockDiagnostic = (
  kind: DataplaneDiagnosticKind,
  status: DataplaneDiagnosticStatus,
  overrides: Partial<DataplaneDiagnosticRead> = {}
): DataplaneDiagnosticRead => ({
  kind,
  target_id: "store-1",
  status,
  checked_at: 1_000,
  updated_at: 1_000,
  ...overrides,
});
