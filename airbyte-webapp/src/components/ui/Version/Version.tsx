import React from "react";

import { Text } from "components/ui/Text";

import { useWebappConfig } from "core/config";

export const Version: React.FC = () => {
  const { version } = useWebappConfig();

  return (
    <Text size="sm" color="grey300" align="center" italicized>
      {version}
    </Text>
  );
};
