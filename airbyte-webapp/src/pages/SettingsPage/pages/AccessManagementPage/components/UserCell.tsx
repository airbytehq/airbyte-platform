import { FormattedMessage } from "react-intl";

import { InitialBadge } from "components/InitialBadge/InitialBadge";
import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
export const UserCell: React.FC<{ name?: string; email: string; isCurrentUser: boolean; userId: string }> = ({
  name,
  email,
  userId,
  isCurrentUser,
}) => {
  const nameToDisplay = name || email;

  return (
    <FlexContainer direction="row" alignItems="center">
      <InitialBadge inputString={nameToDisplay} hashingString={userId} />
      <FlexContainer direction="column" gap="xs">
        <FlexContainer direction="row">
          <Text size="sm">{nameToDisplay}</Text>
          {isCurrentUser && (
            <Badge variant="grey">
              <FormattedMessage id="settings.accessManagement.youHint" />
            </Badge>
          )}
        </FlexContainer>
        {name && (
          <Text size="sm" color="grey400">
            {email}
          </Text>
        )}
      </FlexContainer>
    </FlexContainer>
  );
};
