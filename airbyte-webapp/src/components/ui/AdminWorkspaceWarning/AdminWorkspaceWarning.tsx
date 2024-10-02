import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useIsForeignWorkspace } from "core/api/cloud";

import styles from "./AdminWorkspaceWarning.module.scss";

export const AdminWorkspaceWarning = () => {
  const isForeignWorkspace = useIsForeignWorkspace();
  const { formatMessage } = useIntl();

  if (isForeignWorkspace) {
    return (
      <FlexContainer
        className={styles.adminWorkspaceWarning}
        alignItems="center"
        justifyContent="center"
        direction="row"
        gap="sm"
      >
        <Tooltip
          containerClassName={styles.adminWorkspaceTooltipWrapper}
          placement="right"
          control={
            <Text as="div" inverseColor bold>
              {formatMessage({ id: "workspace.adminWorkspaceWarning" })}
            </Text>
          }
        >
          <FormattedMessage id="workspace.adminWorkspaceWarningTooltip" />
        </Tooltip>
      </FlexContainer>
    );
  }

  return null;
};
