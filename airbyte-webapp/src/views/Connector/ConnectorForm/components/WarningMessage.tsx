import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Message } from "components/ui/Message";

import { useIsFCPEnabled } from "core/api/cloud";
import { ReleaseStage, SupportLevel } from "core/request/AirbyteClient";

export const WarningMessage: React.FC<{ supportLevel?: SupportLevel; releaseStage?: ReleaseStage }> = ({
  supportLevel,
  releaseStage,
}) => {
  const isFCPEnabled = useIsFCPEnabled();

  if (
    (isFCPEnabled && releaseStage !== "alpha" && releaseStage !== "beta") ||
    (!isFCPEnabled && supportLevel !== "community")
  ) {
    return null;
  }

  return (
    <Box>
      <Message
        text={
          <>
            <FormattedMessage
              id={
                isFCPEnabled
                  ? `connector.releaseStage.${releaseStage}.description`
                  : "connector.supportLevel.community.description"
              }
            />{" "}
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
