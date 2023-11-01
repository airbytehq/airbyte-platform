import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentWorkspace, useUpdateWorkspace } from "core/api";
import { NotificationSettings } from "core/request/AirbyteClient";

export const useUpdateNotificationSettings = () => {
  const workspaceId = useCurrentWorkspaceId();
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();

  return (notificationSettings: NotificationSettings) =>
    updateWorkspace({
      workspaceId,
      notificationSettings,
    });
};

export { useCurrentWorkspace };
