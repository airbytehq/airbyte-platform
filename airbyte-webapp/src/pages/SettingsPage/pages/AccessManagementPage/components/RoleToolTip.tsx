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

export const RoleToolTip: React.FC<{ resourceType: ResourceType }> = ({ resourceType }) => {
  return (
    <InfoTooltip>
      {permissionsByResourceType[resourceType].map((permission) => {
        return (
          <Box py="sm" key={permission}>
            <Text inverseColor bold>
              <FormattedMessage id={permissionStringDictionary[permission]} />
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
