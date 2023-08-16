import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace } from "core/api";
import { useOrganization } from "core/api";

export const GeneralOrganizationSettingsPage: React.FC = () => {
  const { organizationId } = useCurrentWorkspace();

  return (
    <Card title={<FormattedMessage id="settings.generalSettings" />}>
      <Box p="xl">{organizationId && <Organization organizationId={organizationId} />}</Box>
    </Card>
  );
};

const Organization = ({ organizationId }: { organizationId: string }) => {
  const organization = useOrganization(organizationId);

  // Placeholder for organization information and settings
  return <Text>{organization.organizationName}</Text>;
};
