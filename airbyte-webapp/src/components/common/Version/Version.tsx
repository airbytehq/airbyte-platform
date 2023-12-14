import React from "react";

import { Text } from "components/ui/Text";

import { useConfig } from "core/config";

export const Version: React.FC = () => {
  const config = useConfig();
  return (
    <Text size="sm" color="grey300" align="center" italicized>
      {config.version}
    </Text>
  );
};
