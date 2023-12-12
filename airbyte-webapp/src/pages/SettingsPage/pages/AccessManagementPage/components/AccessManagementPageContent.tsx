import { FormattedMessage, useIntl } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo, useCurrentWorkspace } from "core/api";
import { useIntent } from "core/utils/rbac";

import { AccessManagementCard } from "./AccessManagementCard";
import styles from "./AccessManagementPageContent.module.scss";
import { AccessUsers, ResourceType } from "./useGetAccessManagementData";

interface AccessManagementContentProps {
  resourceName: string;
  accessUsers: AccessUsers;
  pageResourceType: ResourceType;
}
export const AccessManagementPageContent: React.FC<AccessManagementContentProps> = ({
  resourceName,
  accessUsers,
  pageResourceType,
}) => {
  const { formatMessage } = useIntl();
  const workspace = useCurrentWorkspace();
  const canListOrganizationUsers = useIntent("ListOrganizationMembers", { organizationId: workspace.organizationId });
  const organizationInfo = useCurrentOrganizationInfo();

  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.accessManagement" }]} />
      <Box pb="md">
        <Text size="lg">
          <FormattedMessage id="settings.accessManagement.resource.description" values={{ resourceName }} />
        </Text>
      </Box>
      <FlexContainer direction="column" gap="xl" className={styles.pageContent}>
        {Object.keys(accessUsers).map((key) => {
          const resourceType = key as keyof typeof accessUsers;
          const data = accessUsers[resourceType];
          const users = data?.users ?? [];
          const usersToAdd = data?.usersToAdd ?? [];

          if (resourceType === "organization" && !canListOrganizationUsers) {
            return (
              <Message
                text={formatMessage(
                  { id: "settings.accessManagement.invisibleOrgUsers" },
                  { organization: organizationInfo?.organizationName }
                )}
              />
            );
          }

          return (
            <AccessManagementCard
              users={users}
              usersToAdd={usersToAdd}
              tableResourceType={resourceType}
              key={resourceType}
              pageResourceType={pageResourceType}
              pageResourceName={resourceName}
            />
          );
        })}
      </FlexContainer>
    </>
  );
};
