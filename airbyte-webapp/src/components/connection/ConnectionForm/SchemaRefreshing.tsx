import classnames from "classnames";
import React, { useEffect } from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useFormatError } from "core/errors";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./SchemaRefreshing.module.scss";

/**
 * Used in "edit" mode to show the schema refreshing state
 */
export const SchemaRefreshing: React.FC<React.PropsWithChildren> = ({ children }) => {
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const { schemaError } = useConnectionFormService();
  const { refreshSchema } = useConnectionFormService();
  const { schemaRefreshing } = useConnectionEditService();

  const formatError = useFormatError();

  useEffect(() => {
    if (!schemaError) {
      return;
    }
    registerNotification({
      id: "schema.refreshing.error",
      type: "error",
      text: formatError(schemaError),
      actionBtnText: <FormattedMessage id="form.tryAgain" />,
      onAction: async () => {
        unregisterNotificationById("schema.refreshing.error");
        await refreshSchema();
      },
    });
  }, [formatError, refreshSchema, registerNotification, schemaError, unregisterNotificationById]);

  return (
    <div className={styles.schemaRefreshing}>
      <div
        className={classnames(styles.schemaRefreshing__backdrop, {
          [styles["schemaRefreshing__backdrop--visible"]]: schemaRefreshing,
        })}
      >
        <FlexContainer justifyContent="center" className={styles.schemaRefreshing__message}>
          <Icon type="loading" color="action" size="sm" />
          <Text color="grey400">
            <FormattedMessage id="connection.updateSchema.loading" />
          </Text>
        </FlexContainer>
      </div>
      {children}
    </div>
  );
};
