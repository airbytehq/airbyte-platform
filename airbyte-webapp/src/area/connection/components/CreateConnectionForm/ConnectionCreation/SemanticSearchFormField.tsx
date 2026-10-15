import uniqueId from "lodash/uniqueId";
import React, { useEffect, useMemo, useState } from "react";
import { Controller, useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { ControlLabels } from "components/ui/LabeledControl";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { FormConnectionFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { FormFieldLayout } from "area/connection/components/ConnectionForm/FormFieldLayout";
import { useConnectionFormService } from "area/connection/utils/ConnectionForm/ConnectionFormService";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  SemanticSearchBackfillDate,
  SemanticSearchBackfillProvider,
  SemanticSearchBackfillStatus,
  SemanticSearchStartBackfill,
  SemanticSearchSwitch,
  useConnectionSemanticSearchAccess,
} from "cloud/components/AgentsOptIn";

import { InputContainer } from "./InputContainer";

interface SemanticSearchFormFieldProps {
  isCreating: boolean;
  sourceDefinitionId: string;
  destinationDefinitionId: string;
  disabled?: boolean;
}

interface SemanticSearchFieldLayoutProps {
  controlId?: string;
  titleId: string;
  descriptionId: string;
}

const SemanticSearchFieldLayout: React.FC<React.PropsWithChildren<SemanticSearchFieldLayoutProps>> = ({
  controlId,
  titleId,
  descriptionId,
  children,
}) => (
  <FormFieldLayout alignItems="flex-start" nextSizing>
    <ControlLabels
      htmlFor={controlId}
      label={
        <FlexContainer direction="column" gap="sm">
          <Text bold>
            <FormattedMessage id={titleId} />
          </Text>
          <Text size="sm" color="grey">
            <FormattedMessage id={descriptionId} />
          </Text>
        </FlexContainer>
      }
    />
    {children}
  </FormFieldLayout>
);

interface SemanticSearchBackfillFieldsProps {
  connectionId: string;
  name: string;
  sourceId: string;
  workspaceId: string;
  disabled?: boolean;
}

// The backfill controls of one connection. The start time is sent with the request only, so it is not a form field.
const SemanticSearchBackfillFields: React.FC<SemanticSearchBackfillFieldsProps> = ({
  connectionId,
  name,
  sourceId,
  workspaceId,
  disabled,
}) => {
  const [dateControlId] = useState(`input-control-${uniqueId()}`);
  const connection = useMemo(() => ({ connectionId, name, source: { sourceId } }), [connectionId, name, sourceId]);
  const connections = useMemo(() => [connection], [connection]);

  return (
    <SemanticSearchBackfillProvider connections={connections}>
      <SemanticSearchFieldLayout
        controlId={dateControlId}
        titleId="connectionForm.semanticSearch.backfillDate.title"
        descriptionId="connectionForm.semanticSearch.backfillDate.description"
      >
        <InputContainer>
          <SemanticSearchBackfillDate
            id={dateControlId}
            connection={connection}
            workspaceId={workspaceId}
            disabled={disabled}
          />
        </InputContainer>
      </SemanticSearchFieldLayout>
      <SemanticSearchFieldLayout
        titleId="connectionForm.semanticSearch.backfill.title"
        descriptionId="connectionForm.semanticSearch.backfill.description"
      >
        <FlexContainer alignItems="center" gap="xl">
          <SemanticSearchStartBackfill connection={connection} workspaceId={workspaceId} disabled={disabled} />
          <SemanticSearchBackfillStatus connection={connection} />
        </FlexContainer>
      </SemanticSearchFieldLayout>
    </SemanticSearchBackfillProvider>
  );
};

// A disabled switch that shows off. The tooltip tells the user why.
const LockedOffSwitch: React.FC<React.PropsWithChildren<{ id: string }>> = ({ id, children }) => (
  <Tooltip placement="bottom" control={<Switch id={id} size="lg" checked={false} disabled />}>
    {children}
  </Tooltip>
);

const OrganizationSemanticSearchOffSwitch: React.FC<{ id: string }> = ({ id }) => (
  <LockedOffSwitch id={id}>
    <FormattedMessage id="connectionForm.semanticSearch.organizationOff" />
  </LockedOffSwitch>
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
    <>
      <SemanticSearchFieldLayout
        controlId={controlId}
        titleId="connectionForm.semanticSearch.title"
        descriptionId="connectionForm.semanticSearch.description"
      >
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
      </SemanticSearchFieldLayout>
      {/* The backfill controls check the edit permission and the connection's semantic search value themselves. */}
      {access !== "orgOff" && (
        <SemanticSearchBackfillFields
          connectionId={connection.connectionId}
          name={connection.name ?? ""}
          sourceId={connection.source.sourceId}
          workspaceId={workspaceId}
          disabled={disabled}
        />
      )}
    </>
  );
};

// The create step has no connection yet, so the value is a form field. CreateConnectionForm saves it after create.
const SemanticSearchCreateField: React.FC<Omit<SemanticSearchFormFieldProps, "isCreating">> = ({
  sourceDefinitionId,
  destinationDefinitionId,
  disabled,
}) => {
  const access = useConnectionSemanticSearchAccess({ sourceDefinitionId, destinationDefinitionId });
  const { control, resetField } = useFormContext<FormConnectionFormValues>();
  const { dirtyFields } = useFormState({ control, name: "semanticSearch" });
  const isChangedByUser = Boolean(dirtyFields.semanticSearch);
  const [controlId] = useState(`input-control-${uniqueId()}`);
  const isShown = access !== "loading" && access !== "hidden";
  const defaultValue = access === "editable";

  // The form's initial values are built before the access is known. Once it is known, store the default as the
  // field's initial value, so the form stays clean. Keep a value the user has changed.
  useEffect(() => {
    if (isShown && !isChangedByUser) {
      resetField("semanticSearch", { defaultValue });
    }
  }, [isShown, isChangedByUser, defaultValue, resetField]);

  if (!isShown) {
    return null;
  }

  return (
    <SemanticSearchFieldLayout
      controlId={controlId}
      titleId="connectionForm.semanticSearch.title"
      descriptionId="connectionForm.semanticSearch.description"
    >
      {/* The locked switches render inside Controller only to register the field. resetField does nothing for an
          unregistered field. */}
      <Controller
        name="semanticSearch"
        control={control}
        render={({ field }) =>
          access === "orgOff" ? (
            <OrganizationSemanticSearchOffSwitch id={controlId} />
          ) : access === "noPermission" ? (
            <LockedOffSwitch id={controlId}>
              <FormattedMessage id="cloud.contextLayer.semanticSearch.connectionNoPermission" />
            </LockedOffSwitch>
          ) : (
            <Switch
              id={controlId}
              size="lg"
              // Until the effect above stores the default, the value is undefined. Show the default meanwhile.
              checked={field.value ?? defaultValue}
              onChange={(event) => field.onChange(event.target.checked)}
              disabled={disabled}
            />
          )
        }
      />
    </SemanticSearchFieldLayout>
  );
};

export const SemanticSearchFormField: React.FC<SemanticSearchFormFieldProps> = ({ isCreating, ...props }) => (
  <React.Suspense>
    {isCreating ? <SemanticSearchCreateField {...props} /> : <SemanticSearchSettingsField {...props} />}
  </React.Suspense>
);
