import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { InitialBadge } from "components/ui/InitialBadge/InitialBadge";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Text } from "components/ui/Text";

import { useListGroupMembers } from "core/api";

interface GroupMembersListProps {
  groupId: string;
  /**
   * Collapsible keeps its panel mounted while collapsed, so this component renders before the card
   * is ever expanded. It gates the fetch rather than the render.
   */
  isExpanded: boolean;
}

export const GroupMembersList: React.FC<GroupMembersListProps> = ({ groupId, isExpanded }) => {
  const { data, isInitialLoading, isError } = useListGroupMembers(groupId, { enabled: isExpanded });

  if (isInitialLoading) {
    return (
      <Box py="md">
        <LoadingSpinner />
      </Box>
    );
  }

  if (isError) {
    return (
      <Box py="md">
        <Text size="sm" color="red">
          <FormattedMessage id="settings.organization.groups.members.error" />
        </Text>
      </Box>
    );
  }

  // GroupApiController.listGroupMembers applies no sort, unlike GroupApiController.listGroups which
  // sorts by name, so without this the row order can change between fetches.
  const members = [...(data?.members ?? [])].sort((a, b) => a.userName.localeCompare(b.userName));

  if (members.length === 0) {
    return (
      <Box py="md">
        <Text size="sm" color="grey">
          <FormattedMessage id="settings.organization.groups.members.empty" />
        </Text>
      </Box>
    );
  }

  return (
    <FlexContainer direction="column" gap="md">
      {members.map((member) => (
        <FlexContainer key={member.memberId} alignItems="center" gap="md">
          <InitialBadge inputString={member.userName} hashingString={member.userId} />
          <FlexItem grow>
            <FlexContainer direction="column" gap="none">
              <Text size="sm">{member.userName}</Text>
              <Text size="xs" color="grey">
                {member.userEmail}
              </Text>
            </FlexContainer>
          </FlexItem>
        </FlexContainer>
      ))}
    </FlexContainer>
  );
};
