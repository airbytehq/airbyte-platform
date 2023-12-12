import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";

import { SupportLevel } from "core/api/types/AirbyteClient";
import { links } from "core/utils/links";

export const WarningMessage: React.FC<{ supportLevel?: SupportLevel }> = ({ supportLevel }) => {
  if (supportLevel !== "community") {
    return null;
  }

  return (
    <Box>
      <Message
        text={
          <>
            <FormattedMessage id="connector.supportLevel.community.description" />
            <FormattedMessage
              id="connector.connectorsInDevelopment.docLink"
              values={{
                lnk: (node: React.ReactNode) => <ExternalLink href={links.connectorSupportLevels}>{node}</ExternalLink>,
              }}
            />
          </>
        }
        type="info"
      />
    </Box>
  );
};
