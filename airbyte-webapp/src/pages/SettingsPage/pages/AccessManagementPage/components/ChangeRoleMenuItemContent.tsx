import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { BrandingBadge } from "components/ui/BrandingBadge";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { PermissionType } from "core/api/types/AirbyteClient";
import { FeatureItem, IfFeatureEnabled } from "core/services/features";

import { permissionDescriptionDictionary, permissionStringDictionary, isTeamsFeaturePermissionType } from "./util";

interface ChangeRoleMenuItemContentProps {
  roleIsInvalid: boolean;
  roleIsActive: boolean;
  permissionType: PermissionType;
}

export const ChangeRoleMenuItemContent: React.FC<ChangeRoleMenuItemContentProps> = ({
  roleIsActive,
  permissionType,
  roleIsInvalid,
}) => {
  return (
    <Box px="md" py="lg">
      <FlexContainer alignItems="center" justifyContent="space-between">
        <FlexItem>
          <FlexContainer alignItems="center">
            <Text color={roleIsInvalid ? "grey300" : undefined}>
              <FormattedMessage id={permissionStringDictionary[permissionType].role} />
            </Text>
            {isTeamsFeaturePermissionType(permissionType) && (
              <IfFeatureEnabled feature={FeatureItem.CloudForTeamsBranding}>
                <BrandingBadge product="cloudForTeams" />
              </IfFeatureEnabled>
            )}
          </FlexContainer>
          <Text color={roleIsInvalid ? "grey300" : "grey"}>
            <FormattedMessage
              id={permissionDescriptionDictionary[permissionType].id}
              values={permissionDescriptionDictionary[permissionType].values}
            />
          </Text>
        </FlexItem>
        {roleIsActive && (
          <FlexItem>
            <Icon type="check" color="primary" size="md" />
          </FlexItem>
        )}
      </FlexContainer>
    </Box>
  );
};
