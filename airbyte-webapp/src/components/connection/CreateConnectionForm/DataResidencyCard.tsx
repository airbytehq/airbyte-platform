import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { DataResidencyDropdown } from "components/forms/DataResidencyDropdown";
import { CollapsibleCard } from "components/ui/CollapsibleCard";
import { ExternalLink } from "components/ui/Link";

import { links } from "core/utils/links";

import { CardFormFieldLayout } from "../ConnectionForm/CardFormFieldLayout";
import { FormConnectionFormValues } from "../ConnectionForm/formConfig";

export const DataResidencyCard = () => {
  const { formatMessage } = useIntl();

  return (
    <CollapsibleCard title={formatMessage({ id: "connection.geographyTitle" })} collapsible={false}>
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
    </CollapsibleCard>
  );
};
