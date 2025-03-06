import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";

interface DestinationLimitReachedModalProps {
  destinationCount: number;
}

export const DestinationLimitReachedModal: React.FC<DestinationLimitReachedModalProps> = ({ destinationCount }) => {
  return (
    <>
      <ModalBody>
        <Text>
          <FormattedMessage id="workspaces.destinationLimitReached.description" values={{ count: destinationCount }} />
        </Text>
      </ModalBody>
      <ModalFooter>
        <FlexContainer justifyContent="flex-end">
          <ExternalLink variant="buttonPrimary" href={links.contactSales}>
            <FormattedMessage id="credits.talkToSales" />
          </ExternalLink>
        </FlexContainer>
      </ModalFooter>
    </>
  );
};
