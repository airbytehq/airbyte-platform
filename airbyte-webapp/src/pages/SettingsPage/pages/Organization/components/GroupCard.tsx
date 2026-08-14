import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Collapsible } from "components/ui/Collapsible";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { GroupRead } from "core/api/types/AirbyteClient";
import { useModalService } from "core/services/Modal";

import { EditGroupPermissionsModal } from "./EditGroupPermissionsModal/EditGroupPermissionsModal";
import styles from "./GroupCard.module.scss";
import { GroupMembersList } from "./GroupMembersList";

interface GroupCardProps {
  group: GroupRead;
}

export const GroupCard: React.FC<GroupCardProps> = ({ group }) => {
  const { formatMessage } = useIntl();
  const organizationId = useCurrentOrganizationId();
  const { openModal } = useModalService();
  const [isExpanded, setIsExpanded] = useState(false);

  const hasMembers = group.memberCount > 0;
  const viewMembersLabel = formatMessage({ id: "settings.organization.groups.viewMembers" });
  const viewMembersAriaLabel = formatMessage(
    { id: "settings.organization.groups.viewMembersForGroup" },
    { name: group.name }
  );
  const actionsAriaLabel = formatMessage({ id: "settings.organization.groups.actions" }, { name: group.name });

  // One option only: frame `2010-19179` also shows "Delete group" in the danger colour, but
  // PLAT-1101 owns it (D4).
  const menuOptions: DropdownMenuOptionType[] = [
    {
      displayName: formatMessage({ id: "settings.organization.groups.editPermissions" }),
      value: "editPermissions",
    },
  ];

  const openEditPermissionsModal = () => {
    openModal<void>({
      title: formatMessage({ id: "settings.organization.groups.editPermissions.title" }, { name: group.name }),
      content: ({ onCancel, onComplete }) => (
        <EditGroupPermissionsModal
          group={group}
          organizationId={organizationId}
          onCancel={onCancel}
          onComplete={() => onComplete()}
        />
      ),
    });
  };

  const onMenuOptionChange = (option: DropdownMenuOptionType) => {
    if (option.value === "editPermissions") {
      openEditPermissionsModal();
    }
  };

  return (
    <div className={styles.card}>
      <Box p="lg" className={styles.header}>
        <FlexContainer justifyContent="space-between" alignItems="center" gap="lg">
          <FlexItem grow>
            <FlexContainer direction="column" gap="xs">
              <Text size="md" bold>
                <FormattedMessage
                  id="settings.organization.groups.nameWithCount"
                  values={{ name: group.name, count: group.memberCount }}
                />
              </Text>
              {group.description && (
                <Text size="sm" color="grey">
                  {group.description}
                </Text>
              )}
            </FlexContainer>
          </FlexItem>
          <DropdownMenu
            placement="bottom-end"
            options={menuOptions}
            onChange={onMenuOptionChange}
            data-testid="group-actions-menu"
          >
            {() => <Button variant="clear" icon="options" aria-label={actionsAriaLabel} />}
          </DropdownMenu>
        </FlexContainer>
      </Box>
      <Box p="lg">
        {hasMembers ? (
          <Collapsible label={viewMembersLabel} aria-label={viewMembersAriaLabel} onClick={setIsExpanded}>
            <GroupMembersList groupId={group.groupId} isExpanded={isExpanded} />
          </Collapsible>
        ) : (
          <Tooltip
            control={
              <button type="button" className={styles.disclosurePlaceholder} aria-label={viewMembersAriaLabel} disabled>
                <span className={styles.disclosurePlaceholder__icon}>
                  <Icon type="chevronRight" />
                </span>
                <Text size="sm" color="grey300">
                  {viewMembersLabel}
                </Text>
              </button>
            }
          >
            <FormattedMessage id="settings.organization.groups.viewMembers.disabled" />
          </Tooltip>
        )}
      </Box>
    </div>
  );
};
