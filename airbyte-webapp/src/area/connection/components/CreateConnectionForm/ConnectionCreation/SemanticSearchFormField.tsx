import uniqueId from "lodash/uniqueId";
import React, { useState } from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { ControlLabels } from "components/ui/LabeledControl";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { FormFieldLayout } from "area/connection/components/ConnectionForm/FormFieldLayout";
import { useConnectionFormService } from "area/connection/utils/ConnectionForm/ConnectionFormService";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { SemanticSearchSwitch, useConnectionSemanticSearchAccess } from "cloud/components/AgentsOptIn";

interface SemanticSearchFormFieldProps {
  isCreating: boolean;
  sourceDefinitionId: string;
  destinationDefinitionId: string;
  disabled?: boolean;
}

const OrganizationSemanticSearchOffSwitch: React.FC<{ id: string }> = ({ id }) => (
  <Tooltip placement="bottom" control={<Switch id={id} size="lg" checked={false} disabled />}>
    <FormattedMessage id="connectionForm.semanticSearch.organizationOff" />
  </Tooltip>
);

// Saves on toggle, apart from the form, because the setting lives outside the connection.
const SemanticSearchSettingsField: React.FC<Omit<SemanticSearchFormFieldProps, "isCreating">> = ({
  sourceDefinitionId,
  destinationDefinitionId,
  disabled,
}) => {
  const access = useConnectionSemanticSearchAccess({ sourceDefinitionId, destinationDefinitionId });
  const { connection } = useConnectionFormService();
  const workspaceId = useCurrentWorkspaceId();
  const [controlId] = useState(`input-control-${uniqueId()}`);

  if (access === "loading" || access === "hidden" || !connection.connectionId) {
    return null;
  }

  return (
    <FormFieldLayout alignItems="flex-start" nextSizing>
      <ControlLabels
        htmlFor={controlId}
        label={
          <FlexContainer direction="column" gap="sm">
            <Text bold>
              <FormattedMessage id="connectionForm.semanticSearch.title" />
            </Text>
            <Text size="sm" color="grey">
              <FormattedMessage id="connectionForm.semanticSearch.description" />
            </Text>
          </FlexContainer>
        }
      />
      {access === "orgOff" ? (
        <OrganizationSemanticSearchOffSwitch id={controlId} />
      ) : (
        // SemanticSearchSwitch checks the edit permission itself, so "noPermission" and "editable" render the same.
        <SemanticSearchSwitch
          id={controlId}
          connectionId={connection.connectionId}
          name={connection.name ?? ""}
          workspaceId={workspaceId}
          size="lg"
          disabled={disabled}
        />
      )}
    </FormFieldLayout>
  );
};

export const SemanticSearchFormField: React.FC<SemanticSearchFormFieldProps> = ({ isCreating, ...props }) => {
  if (isCreating) {
    return null;
  }

  return (
    <React.Suspense>
      <SemanticSearchSettingsField {...props} />
    </React.Suspense>
  );
};
