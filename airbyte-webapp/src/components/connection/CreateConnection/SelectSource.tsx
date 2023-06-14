import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useSourceList } from "hooks/services/useSourceHook";

import { CreateNewSource, SOURCE_DEFINITION_PARAM } from "./CreateNewSource";
import { RadioButtonTiles } from "./RadioButtonTiles";
import { SelectExistingConnector } from "./SelectExistingConnector";

export type SourceType = "existing" | "new";

export const EXISTING_SOURCE_TYPE = "existing";
export const NEW_SOURCE_TYPE = "new";
export const SOURCE_TYPE_PARAM = "sourceType";
export const SOURCE_ID_PARAM = "sourceId";

export const SelectSource: React.FC = () => {
  const { sources } = useSourceList();
  const [searchParams, setSearchParams] = useSearchParams();

  if (!searchParams.get(SOURCE_TYPE_PARAM)) {
    if (sources.length === 0) {
      searchParams.set(SOURCE_TYPE_PARAM, NEW_SOURCE_TYPE);
      setSearchParams(searchParams);
    } else {
      searchParams.set(SOURCE_TYPE_PARAM, EXISTING_SOURCE_TYPE);
      setSearchParams(searchParams);
    }
  }
  const selectedSourceType = useMemo(() => {
    return searchParams.get(SOURCE_TYPE_PARAM) as SourceType;
  }, [searchParams]);

  const { hasFormChanges } = useFormChangeTrackerService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const selectSourceType = (sourceType: SourceType) => {
    searchParams.delete(SOURCE_DEFINITION_PARAM);
    searchParams.set(SOURCE_TYPE_PARAM, sourceType);
    setSearchParams(searchParams);
  };

  const selectSource = (sourceId: string) => {
    searchParams.delete(SOURCE_TYPE_PARAM);
    searchParams.set(SOURCE_ID_PARAM, sourceId);
    setSearchParams(searchParams);
  };

  const onSelectSourceType = (sourceType: SourceType) => {
    if (hasFormChanges) {
      openConfirmationModal({
        title: "form.discardChanges",
        text: "form.discardChangesConfirmation",
        submitButtonText: "form.discardChanges",
        onSubmit: () => {
          closeConfirmationModal();
          selectSourceType(sourceType);
        },
        onClose: () => {
          selectSourceType(sourceType);
        },
      });
    } else {
      selectSourceType(sourceType);
    }
  };

  return (
    <>
      <Card withPadding>
        <Heading as="h2">
          <FormattedMessage id="connectionForm.defineSource" />
        </Heading>
        <Box mt="md">
          <RadioButtonTiles
            name="sourceType"
            options={[
              {
                value: EXISTING_SOURCE_TYPE,
                label: "connectionForm.sourceExisting",
                description: "connectionForm.sourceExistingDescription",
                disabled: sources.length === 0,
              },
              {
                value: NEW_SOURCE_TYPE,
                label: "onboarding.sourceSetUp",
                description: "onboarding.sourceSetUp.description",
              },
            ]}
            selectedValue={selectedSourceType}
            onSelectRadioButton={(id) => onSelectSourceType(id)}
          />
        </Box>
      </Card>
      <Box mt="xl">
        {selectedSourceType === EXISTING_SOURCE_TYPE && (
          <SelectExistingConnector connectors={sources} selectConnector={selectSource} />
        )}
        {selectedSourceType === NEW_SOURCE_TYPE && <CreateNewSource />}
      </Box>
      <CloudInviteUsersHint connectorType="source" />
    </>
  );
};
