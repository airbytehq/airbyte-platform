import { FormattedMessage } from "react-intl";

import { Icon } from "components/ui/Icon";
import { ExternalLink, Link } from "components/ui/Link";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useCurrentConnection } from "core/api";
import { RoutePaths } from "pages/routePaths";

import styles from "./RejectedRecordsLink.module.scss";

interface RejectedRecordsLinkProps {
  recordsRejected: number;
  cloudConsoleUrl?: string;
}

export const RejectedRecordsLink: React.FC<RejectedRecordsLinkProps> = ({ recordsRejected, cloudConsoleUrl }) => {
  const { destination } = useCurrentConnection();
  const createWorkspaceLink = useCurrentWorkspaceLink();
  const linkToDestinationSetup = createWorkspaceLink(`/${RoutePaths.Destination}/${destination?.destinationId}`);

  if (!cloudConsoleUrl) {
    return (
      <Tooltip
        control={
          <Link to={linkToDestinationSetup} className={styles.rejectedRecordsLink}>
            <TextContent recordsRejected={recordsRejected} />
          </Link>
        }
      >
        <FormattedMessage id="sources.setUpRejectedRecordsBucket" />
      </Tooltip>
    );
  }

  return (
    <ExternalLink href={cloudConsoleUrl} opensInNewTab className={styles.rejectedRecordsLink}>
      <TextContent recordsRejected={recordsRejected} />
    </ExternalLink>
  );
};

const TextContent = ({ recordsRejected }: { recordsRejected: number }) => {
  return (
    <>
      <Icon type="warningOutline" color="primary" size="xs" className={styles.rejectedRecordsLink__icon} />
      <Text as="span" color="blue" size="sm">
        <FormattedMessage id="sources.countRecordsRejected" values={{ count: recordsRejected }} />
      </Text>
    </>
  );
};
