import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

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
          const users = accessUsers[resourceType];

          return (
            users &&
            users.length > 0 && (
              <AccessManagementCard
                users={users}
                tableResourceType={resourceType}
                key={resourceType}
                pageResourceType={pageResourceType}
                pageResourceName={resourceName}
              />
            )
          );
        })}
      </FlexContainer>
    </>
  );
};
