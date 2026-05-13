import { useListPermissions } from "core/api";
import { useCurrentUser } from "core/services/auth";

export const useIsInstanceAdmin = (): boolean => {
  const { userId } = useCurrentUser();
  const { permissions } = useListPermissions(userId);

  return permissions.some((permission) => permission.permissionType === "instance_admin");
};
