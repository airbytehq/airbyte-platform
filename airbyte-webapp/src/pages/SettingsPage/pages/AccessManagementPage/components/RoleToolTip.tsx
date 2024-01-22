import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import {
  ResourceType,
  permissionDescriptionDictionary,
  permissionStringDictionary,
  permissionsByResourceType,
} from "./useGetAccessManagementData";
/**
 * @deprecated this component will be removed when RBAC UI v2 is turned on. Role descriptions will live in the RoleManagementMenu instead.
 */
export const RoleToolTip: React.FC<{ resourceType: ResourceType }> = ({ resourceType }) => {
  return (
    <InfoTooltip>
      {permissionsByResourceType[resourceType].map((permission) => {
        return (
          <Box py="sm" key={permission}>
            <Text inverseColor bold>
              <FormattedMessage id={permissionStringDictionary[permission].role} />
            </Text>
            <Text inverseColor>
              <FormattedMessage
                id={permissionDescriptionDictionary[permission].id}
                values={permissionDescriptionDictionary[permission].values}
              />
            </Text>
          </Box>
        );
      })}
    </InfoTooltip>
  );
};
