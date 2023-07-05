import { ToastProps } from "components/ui/Toast";

export interface Notification extends Pick<ToastProps, "type" | "onAction" | "onClose" | "actionBtnText" | "text"> {
  id: string;
  /**
   * Whether this notification should time out automatically. If unspecified will be `true` except for `type: error` notifications,
   * where it will be `false` if not specified.
   */
  timeout?: boolean;
}

export interface NotificationServiceApi {
  addNotification: (notification: Notification) => void;
  deleteNotificationById: (notificationId: string | number) => void;
  clearAll: () => void;
}

export interface NotificationServiceState {
  notifications: Notification[];
}
