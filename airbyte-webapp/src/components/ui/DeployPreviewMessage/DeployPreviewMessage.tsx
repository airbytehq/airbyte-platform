import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";

import styles from "./DeployPreviewMessage.module.scss";

const PULL_REQUEST_NUMBER = window.location.hostname.match(/deploy-preview-(\d+)/)?.[1] ?? null;

export const DeployPreviewMessage: React.FC = () => {
  const [isExpanded, setIsExpanded] = useState(true);

  if (!PULL_REQUEST_NUMBER) {
    return null;
  }

  return (
    <FlexContainer className={styles.deployPreviewMessage} alignItems="stretch" gap="none">
      {isExpanded && (
        <Box py="sm" pl="md">
          <FormattedMessage
            id="webapp.deploy-preview-message"
            values={{
              pullRequestNumber: PULL_REQUEST_NUMBER,
              link: (children) => (
                <ExternalLink
                  href={`https://github.com/airbytehq/airbyte-platform-internal/pull/${PULL_REQUEST_NUMBER}`}
                >
                  {children}
                </ExternalLink>
              ),
            }}
          />
        </Box>
      )}
      <button onClick={() => setIsExpanded((s) => !s)} className={styles.deployPreviewMessage__button}>
        <Icon type={isExpanded ? "chevronLeft" : "chevronUp"} />
      </button>
    </FlexContainer>
  );
};
