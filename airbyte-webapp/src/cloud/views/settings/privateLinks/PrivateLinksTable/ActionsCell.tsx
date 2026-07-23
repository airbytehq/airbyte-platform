import React from "react";
import { useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";

import { PrivateLinkRead, PrivateLinkStatus } from "core/api/types/AirbyteClient";

const VIEW_STATUSES: ReadonlySet<PrivateLinkStatus> = new Set([PrivateLinkStatus.available]);
const DELETE_STATUSES: ReadonlySet<PrivateLinkStatus> = new Set([
  PrivateLinkStatus.available,
  PrivateLinkStatus.pending_acceptance,
  PrivateLinkStatus.configuring,
  PrivateLinkStatus.create_failed,
  PrivateLinkStatus.delete_failed,
]);

interface ActionsCellProps {
  link: PrivateLinkRead;
  onDelete: (link: PrivateLinkRead) => void;
  onViewDetails: (link: PrivateLinkRead) => void;
}

export const ActionsCell: React.FC<ActionsCellProps> = ({ link, onDelete, onViewDetails }) => {
  const { formatMessage } = useIntl();
  const canView = VIEW_STATUSES.has(link.status);
  const canDelete = DELETE_STATUSES.has(link.status);

  if (!canView && !canDelete) {
    return null;
  }

  const options: DropdownMenuOptionType[] = [];
  if (canView) {
    options.push({
      displayName: formatMessage({ id: "settings.privateLinks.actions.viewDetails" }),
      value: "view",
    });
  }
  if (canDelete) {
    options.push({
      displayName: formatMessage({ id: "settings.privateLinks.actions.delete" }),
      value: "delete",
    });
  }

  const onOptionClick = (option: DropdownMenuOptionType) => {
    if (option.value === "view") {
      onViewDetails(link);
    } else if (option.value === "delete") {
      onDelete(link);
    }
  };

  return (
    <FlexContainer justifyContent="flex-end">
      <DropdownMenu placement="bottom-end" options={options} onChange={onOptionClick}>
        {() => (
          <Button
            variant="clear"
            icon="options"
            aria-label={formatMessage({ id: "settings.privateLinks.actions.menuLabel" })}
          />
        )}
      </DropdownMenu>
    </FlexContainer>
  );
};
