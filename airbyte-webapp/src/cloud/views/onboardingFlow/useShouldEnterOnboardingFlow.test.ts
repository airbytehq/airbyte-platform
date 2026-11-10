import { OrganizationOnboardingStatus, PermissionRead, PermissionType } from "core/api/types/AirbyteClient";

import {
  canOpenOnboardingFlow,
  hasOrganizationEditorRole,
  shouldEnterOnboardingFlow,
} from "./useShouldEnterOnboardingFlow";

const editor = { canUpdateStatus: true, isAdpOrganization: false, hasDirectEditorRole: true };

const permission = (permissionType: PermissionType, organizationId?: string): PermissionRead => ({
  permissionId: "permission-id",
  permissionType,
  organizationId,
});

describe("shouldEnterOnboardingFlow", () => {
  it.each<OrganizationOnboardingStatus>([
    "survey",
    "choose_method",
    "create_source",
    "create_destination",
    "create_connection",
    "run_first_sync",
  ])("enters the flow at %s", (status) => {
    expect(shouldEnterOnboardingFlow({ ...editor, status })).toBe(true);
  });

  it.each<OrganizationOnboardingStatus | undefined>([undefined, "completed", "skipped"])(
    "stays in the app at %s",
    (status) => {
      expect(shouldEnterOnboardingFlow({ ...editor, status })).toBe(false);
    }
  );

  it("keeps out a member who cannot update the status", () => {
    expect(shouldEnterOnboardingFlow({ ...editor, status: "survey", canUpdateStatus: false })).toBe(false);
  });

  it("does not redirect support staff, and lets them open the flow URL", () => {
    const support = { ...editor, status: "survey" as const, hasDirectEditorRole: false };
    expect(shouldEnterOnboardingFlow(support)).toBe(false);
    expect(canOpenOnboardingFlow(support)).toBe(true);
  });

  it("counts only an editor role on the current organization", () => {
    const instanceAdmin = permission("instance_admin");
    expect(hasOrganizationEditorRole([instanceAdmin, permission("organization_member", "org")], "org")).toBe(false);
    expect(hasOrganizationEditorRole([instanceAdmin, permission("organization_editor", "other")], "org")).toBe(false);
  });

  it.each<PermissionType>(["organization_admin", "organization_editor"])(
    "counts %s on the current organization",
    (role) => {
      expect(hasOrganizationEditorRole([permission("instance_admin"), permission(role, "org")], "org")).toBe(true);
    }
  );

  it("keeps an ADP organization out of the flow", () => {
    expect(shouldEnterOnboardingFlow({ ...editor, status: "survey", isAdpOrganization: true })).toBe(false);
  });
});
