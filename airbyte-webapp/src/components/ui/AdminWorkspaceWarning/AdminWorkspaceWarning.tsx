import { FormattedMessage, useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import { useIsForeignWorkspace } from "core/api/cloud";

import styles from "./AdminWorkspaceWarning.module.scss";
import { Tooltip } from "../Tooltip";

export const AdminWorkspaceWarning = () => {
  const isForeignWorkspace = useIsForeignWorkspace();
  const { formatMessage } = useIntl();

  if (isForeignWorkspace) {
    return (
      <div className={styles.adminWorkspaceWarning}>
        <Tooltip
          placement="right"
          control={
            <div className={styles.adminWorkspaceWarning__pill}>
              <Text inverseColor>{formatMessage({ id: "workspace.adminWorkspaceWarning" })}</Text>
            </div>
          }
        >
          <FormattedMessage id="workspace.adminWorkspaceWarningTooltip" />
        </Tooltip>
      </div>
    );
  }

  return null;
};
