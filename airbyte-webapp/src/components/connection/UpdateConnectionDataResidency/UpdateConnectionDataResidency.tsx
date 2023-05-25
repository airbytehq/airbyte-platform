import * as Flags from "country-flag-icons/react/3x2";
import React from "react";
import { useIntl } from "react-intl";
import { FormattedMessage } from "react-intl";

import { FormControl } from "components/forms";
import { ExternalLink } from "components/ui/Link";

import { useAvailableGeographies } from "core/api";
import { Geography } from "core/request/AirbyteClient";
import { ConnectionSettingsFormValues } from "pages/connections/ConnectionSettingsPage/ConnectionSettingsPage";
import { links } from "utils/links";

import styles from "./UpdateConnectionDataResidency.module.scss";

const fieldDescription = (
  <FormattedMessage
    id="connection.geographyDescription"
    values={{
      ipLink: (node: React.ReactNode) => <ExternalLink href={links.cloudAllowlistIPsLink}>{node}</ExternalLink>,
      docLink: (
        <ExternalLink href={links.connectionDataResidency}>
          <FormattedMessage id="ui.learnMore" />
        </ExternalLink>
      ),
    }}
  />
);

export const UpdateConnectionDataResidency: React.FC = () => {
  const { formatMessage } = useIntl();

  const { geographies } = useAvailableGeographies();

  const options = geographies.map((geography) => {
    const Flag =
      geography === "auto" ? Flags.US : Flags[geography.toUpperCase() as Uppercase<Exclude<Geography, "auto">>];
    return {
      label: formatMessage({
        id: `connection.geography.${geography}`,
        defaultMessage: geography.toUpperCase(),
      }),
      value: geography,
      icon: <Flag className={styles.flag} />,
    };
  });

  return (
    <FormControl<ConnectionSettingsFormValues>
      label={formatMessage({ id: "connection.geographyTitle" })}
      description={fieldDescription}
      fieldType="dropdown"
      name="geography"
      options={options}
    />
  );
};
