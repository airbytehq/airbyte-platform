import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms";
import { CollapsibleCard } from "components/ui/CollapsibleCard";

import { CardFormFieldLayout } from "../ConnectionForm/CardFormFieldLayout";

export const ConnectionNameCard: React.FC = () => {
  const { formatMessage } = useIntl();

  return (
    <CollapsibleCard title={<FormattedMessage id="connection.title" />} collapsible={false}>
      <CardFormFieldLayout>
        <FormControl
          name="name"
          fieldType="input"
          label={formatMessage({ id: "form.connectionName" })}
          placeholder={formatMessage({ id: "form.connectionName.placeholder" })}
          labelTooltip={formatMessage({ id: "form.connectionName.message" })}
          data-testid="connectionName"
          inline
        />
      </CardFormFieldLayout>
    </CollapsibleCard>
  );
};
