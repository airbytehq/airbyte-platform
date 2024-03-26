import React from "react";

import { Text } from "components/ui/Text";

import { config } from "core/config";

export const Version: React.FC = () => {
  return (
    <Text size="sm" color="grey300" align="center" italicized>
      {config.version}
    </Text>
  );
};
