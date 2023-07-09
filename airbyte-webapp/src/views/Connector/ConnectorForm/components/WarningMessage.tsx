import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";

import { ReleaseStage } from "core/request/AirbyteClient";
import { links } from "utils/links";

interface WarningMessageProps {
  stage: typeof ReleaseStage.alpha | typeof ReleaseStage.beta;
}

export const WarningMessage: React.FC<WarningMessageProps> = ({ stage }) => {
  return (
    <Box px="lg">
      <Message
        text={
          <>
            <FormattedMessage id={`connector.releaseStage.${stage}.description`} />{" "}
            <FormattedMessage
              id="connector.connectorsInDevelopment.docLink"
              values={{
                lnk: (node: React.ReactNode) => <ExternalLink href={links.productReleaseStages}>{node}</ExternalLink>,
              }}
            />
          </>
        }
        type="warning"
      />
    </Box>
  );
};
