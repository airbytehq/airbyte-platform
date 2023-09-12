import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Message } from "components/ui/Message";

export const WarningMessage: React.FC<unknown> = () => {
  return (
    <Box>
      <Message
        text={
          <>
            <FormattedMessage id="connector.supportLevel.community.description" />{" "}
            {/* 
            TODO: re-enable and link to the new connector certification docs page when it's ready
            <FormattedMessage
              id="connector.connectorsInDevelopment.docLink"
              values={{
                lnk: (node: React.ReactNode) => <ExternalLink href={links.productReleaseStages}>{node}</ExternalLink>,
              }}
            /> */}
          </>
        }
        type="info"
      />
    </Box>
  );
};
