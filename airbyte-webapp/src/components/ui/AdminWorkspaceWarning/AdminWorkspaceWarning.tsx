import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useIsInstanceAdminEnabled, useSetIsInstanceAdminEnabled } from "core/api";
import { useIsForeignWorkspace } from "core/api/cloud";

import styles from "./AdminWorkspaceWarning.module.scss";

export const AdminWorkspaceWarning = () => {
  const isForeignWorkspace = useIsForeignWorkspace();
  const isInstanceAdminEnabled = useIsInstanceAdminEnabled();
  const setIsInstanceAdminEnabled = useSetIsInstanceAdminEnabled();
  const { formatMessage } = useIntl();

  const CustomListBoxButton: React.FC = useMemo(
    () =>
      // eslint-disable-next-line react/display-name
      <T,>({ selectedOption }: ListBoxControlButtonProps<T>) => (
        <FlexContainer alignItems="center" gap="xs">
          <Icon size="sm" type={isInstanceAdminEnabled ? "pencil" : "eye"} color="foreground" />
          <Text inverseColor>{selectedOption ? selectedOption.label : formatMessage({ id: "form.selectValue" })}</Text>
          <Icon type="caretDown" color="foreground" />
        </FlexContainer>
      ),
    [formatMessage, isInstanceAdminEnabled]
  );
  CustomListBoxButton.displayName = "CustomListBoxButton";

  if (isForeignWorkspace) {
    return (
      <FlexContainer
        className={styles.adminWorkspaceWarning}
        alignItems="center"
        justifyContent="space-between"
        direction="row"
        gap="sm"
      >
        <Tooltip
          containerClassName={styles.adminWorkspaceTooltipWrapper}
          placement="right"
          control={
            <Text as="div" inverseColor bold align="center">
              {formatMessage({ id: "workspace.adminWorkspaceWarning" })}
            </Text>
          }
        >
          <FormattedMessage id="workspace.adminWorkspaceWarningTooltip" />
        </Tooltip>
        <ListBox
          onSelect={(selectedValue) => setIsInstanceAdminEnabled(selectedValue !== "viewing")}
          options={[
            { icon: <Icon type="eye" />, label: formatMessage({ id: "workspace.viewing" }), value: "viewing" },
            { icon: <Icon type="pencil" />, label: formatMessage({ id: "workspace.editing" }), value: "editing" },
          ]}
          controlButton={CustomListBoxButton}
          optionsMenuClassName={styles.optionsMenuClassName}
          buttonClassName={styles.listBoxControlButton}
          selectedValue={isInstanceAdminEnabled ? "editing" : "viewing"}
          adaptiveWidth={false}
        />
      </FlexContainer>
    );
  }

  return null;
};
