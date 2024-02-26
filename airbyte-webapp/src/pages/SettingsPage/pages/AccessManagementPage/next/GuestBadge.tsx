import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Tooltip } from "components/ui/Tooltip";

import { useListUsersInOrganization } from "core/api";

export const GuestBadge: React.FC<{ userId: string; organizationId: string }> = ({ userId, organizationId }) => {
  const { users } = useListUsersInOrganization(organizationId);

  if (users.find((orgUser) => orgUser.userId === userId)) {
    return null;
  }

  return (
    <Tooltip
      control={
        <Badge variant="grey">
          <FormattedMessage id="role.guest" />
        </Badge>
      }
    >
      <FormattedMessage id="settings.accessManagement.guestUser" />
    </Tooltip>
  );
};
