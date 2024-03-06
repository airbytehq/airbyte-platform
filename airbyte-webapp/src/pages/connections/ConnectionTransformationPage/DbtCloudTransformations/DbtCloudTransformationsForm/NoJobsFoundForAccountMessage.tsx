import React from "react";
import { FormattedMessage } from "react-intl";

import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";

/**
 * if there are no jobs from dbt cloud
 */
export const NoJobsFoundForAccountMsg: React.FC = () => (
  <Text color="grey">
    <FormattedMessage
      id="connection.dbtCloudJobs.noJobsFoundForAccount"
      values={{
        lnk: (lnk: React.ReactNode[]) => <ExternalLink href={links.dbtCloud}>{lnk}</ExternalLink>,
      }}
    />
  </Text>
);
