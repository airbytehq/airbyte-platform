import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";

export const Disclaimer: React.FC = () => (
  <Box mt="xl">
    <Text>
      <FormattedMessage
        id="login.disclaimer"
        values={{
          terms: (terms: React.ReactNode) => (
            <ExternalLink href={links.termsLink} variant="primary">
              {terms}
            </ExternalLink>
          ),
          privacy: (privacy: React.ReactNode) => (
            <ExternalLink href={links.privacyLink} variant="primary">
              {privacy}
            </ExternalLink>
          ),
        }}
      />
    </Text>
  </Box>
);
