import { FlexContainer } from "components/ui/Flex";
import { ModalBody } from "components/ui/Modal";

import { CursorFieldsDiffSection } from "./CursorFieldsDiffSection";
import { FieldsDataTypeDiffSection } from "./FieldsDataTypeDiffSection";
import { PrimaryKeysDiffSection } from "./PrimaryKeysDiffSection";
import { StreamAndFieldDiffSection } from "./StreamAndFieldDiffSection";
import { SyncModesDiffSection } from "./SyncModesDiffSection";
import { CatalogConfigDiffExtended } from "../CatalogChangeEventItem";

interface CatalogConfigDiffModalProps {
  catalogConfigDiff: CatalogConfigDiffExtended;
}

export const CatalogConfigDiffModal: React.FC<CatalogConfigDiffModalProps> = ({ catalogConfigDiff }) => {
  const {
    streamsAdded = [],
    fieldsAdded = [],
    streamsRemoved = [],
    fieldsRemoved = [],
    streamsEnabled = [],
    fieldsEnabled = [],
    streamsDisabled = [],
    fieldsDisabled = [],
    syncModesChanged = [],
    cursorFieldsChanged = [],
    primaryKeysChanged = [],
    fieldsDataTypeChanged = [],
  } = catalogConfigDiff;

  return (
    <ModalBody maxHeight="80vh">
      <FlexContainer direction="column" gap="xl">
        <StreamAndFieldDiffSection
          streams={streamsAdded}
          fields={fieldsAdded}
          messageId="connection.timeline.connection_schema_update.catalog_config_diff.streamsAndFieldsAdded"
          iconType="plus"
          iconColor="success"
        />
        <StreamAndFieldDiffSection
          streams={streamsRemoved}
          fields={fieldsRemoved}
          messageId="connection.timeline.connection_schema_update.catalog_config_diff.streamsAndFieldsRemoved"
          iconType="minus"
          iconColor="error"
        />
        <StreamAndFieldDiffSection
          streams={streamsEnabled}
          fields={fieldsEnabled}
          messageId="connection.timeline.connection_schema_update.catalog_config_diff.streamsAndFieldsEnabled"
          iconType="plus"
          iconColor="success"
          backgroundColor="enabled"
        />
        <StreamAndFieldDiffSection
          streams={streamsDisabled}
          fields={fieldsDisabled}
          messageId="connection.timeline.connection_schema_update.catalog_config_diff.streamsAndFieldsDisabled"
          iconType="minus"
          iconColor="error"
          backgroundColor="disabled"
        />
        <SyncModesDiffSection syncModes={syncModesChanged} />
        <CursorFieldsDiffSection cursorFields={cursorFieldsChanged} />
        <PrimaryKeysDiffSection primaryKeys={primaryKeysChanged} />
        <FieldsDataTypeDiffSection fieldsDataType={fieldsDataTypeChanged} />
      </FlexContainer>
    </ModalBody>
  );
};
