import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";

interface SourceLimitReachedModalProps {
  sourceCount: number;
}

export const SourceLimitReachedModal: React.FC<SourceLimitReachedModalProps> = ({ sourceCount }) => {
  return (
    <>
      <ModalBody>
        <Text>
          <FormattedMessage id="workspaces.sourceLimitReached.description" values={{ count: sourceCount }} />
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
