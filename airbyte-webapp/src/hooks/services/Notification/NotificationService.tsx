import { motion, AnimatePresence } from "framer-motion";
import React, { useCallback, useContext, useMemo } from "react";
import { createPortal } from "react-dom";

import { Toast } from "components/ui/Toast";

import useTypesafeReducer from "hooks/useTypesafeReducer";

import styles from "./NotificationService.module.scss";
import { actions, initialState, notificationServiceReducer } from "./reducer";
import { Notification, NotificationServiceApi, NotificationServiceState } from "./types";

const notificationServiceContext = React.createContext<NotificationServiceApi | null>(null);

export const NotificationService = React.memo(({ children }: { children: React.ReactNode }) => {
  const [state, { addNotification, clearAll, deleteNotificationById }] = useTypesafeReducer<
    NotificationServiceState,
    typeof actions
  >(notificationServiceReducer, initialState, actions);

  const baseNotificationService: NotificationServiceApi = useMemo(
    () => ({
      addNotification,
      deleteNotificationById,
      clearAll,
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );

  const registerNotification = useCallback(
    (notification: Notification) => {
      addNotification({ ...notification, timeout: notification.timeout ?? notification.type !== "error" });
    },
    [addNotification]
  );

  const notificationService = useMemo(
    () => ({
      ...baseNotificationService,
      addNotification: registerNotification,
    }),
    [baseNotificationService, registerNotification]
  );

  return (
    <>
      <notificationServiceContext.Provider value={notificationService}>{children}</notificationServiceContext.Provider>
      {createPortal(
        <motion.div className={styles.notifications}>
          <AnimatePresence>
            {state.notifications
              .slice()
              .reverse()
              .map((notification) => {
                return (
                  <motion.div
                    layout="position"
                    key={notification.id}
                    initial={{ scale: 0.9, opacity: 0 }}
                    animate={{ scale: 1, opacity: 1, transition: { delay: 0.1 } }}
                    exit={{ scale: 0.9, opacity: 0 }}
                    transition={{ ease: "easeOut" }}
                  >
                    <Toast
                      text={notification.text}
                      type={notification.type}
                      timeout={notification.timeout}
                      actionBtnText={notification.actionBtnText}
                      onAction={notification.onAction}
                      data-testid={`notification-${notification.id}`}
                      onClose={() => {
                        deleteNotificationById(notification.id);
                        notification.onClose?.();
                      }}
                    />
                  </motion.div>
                );
              })}
          </AnimatePresence>
        </motion.div>,
        document.body
      )}
    </>
  );
});
NotificationService.displayName = "NotificationService";

interface NotificationServiceHook {
  registerNotification: (notification: Notification) => void;
  unregisterAllNotifications: () => void;
  unregisterNotificationById: (notificationId: string | number) => void;
}

export const useNotificationService = (): NotificationServiceHook => {
  const notificationService = useContext(notificationServiceContext);
  if (!notificationService) {
    throw new Error("useNotificationService must be used within a NotificationService.");
  }

  return {
    registerNotification: notificationService.addNotification,
    unregisterNotificationById: notificationService.deleteNotificationById,
    unregisterAllNotifications: notificationService.clearAll,
  };
};
