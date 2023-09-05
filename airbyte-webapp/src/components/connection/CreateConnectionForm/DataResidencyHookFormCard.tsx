import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { DataResidencyDropdown } from "components/forms/DataResidencyDropdown";
import { CollapsibleCard } from "components/ui/CollapsibleCard";
import { ExternalLink } from "components/ui/Link";

import { links } from "core/utils/links";

import { HookFormConnectionFormValues } from "../ConnectionForm/hookFormConfig";
import { HookFormFieldLayout } from "../ConnectionForm/HookFormFieldLayout";

/**
 * react-hook-form version of DataResidency
 * @constructor
 */
export const DataResidencyHookFormCard = () => {
  const { formatMessage } = useIntl();

  return (
    <CollapsibleCard title={formatMessage({ id: "connection.geographyTitle" })} collapsible={false}>
      <HookFormFieldLayout>
        <DataResidencyDropdown<HookFormConnectionFormValues>
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
      </HookFormFieldLayout>
    </CollapsibleCard>
  );
};
