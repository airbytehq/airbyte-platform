import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { DataResidencyDropdown } from "components/forms/DataResidencyDropdown";
import { Card } from "components/ui/Card";
import { ExternalLink } from "components/ui/Link";

import { links } from "core/utils/links";

import { CardFormFieldLayout } from "../ConnectionForm/CardFormFieldLayout";
import { FormConnectionFormValues } from "../ConnectionForm/formConfig";

export const DataResidencyCard = () => {
  const { formatMessage } = useIntl();

  return (
    <Card title={formatMessage({ id: "connection.geographyTitle" })}>
      <CardFormFieldLayout>
        <DataResidencyDropdown<FormConnectionFormValues>
          name="geography"
          inline
          labelId={formatMessage({ id: "connection.geographyTitle" })}
          labelTooltip={
            <FormattedMessage
              id="connection.geographyDescription"
              values={{
                ipLink: (node: React.ReactNode) => (
                  <ExternalLink href={links.cloudAllowlistIPsLink}>{node}</ExternalLink>
                ),
                docLink: (node: React.ReactNode) => (
                  <ExternalLink href={links.connectionDataResidency}>{node}</ExternalLink>
                ),
              }}
            />
          }
        />
      </CardFormFieldLayout>
    </Card>
  );
};
