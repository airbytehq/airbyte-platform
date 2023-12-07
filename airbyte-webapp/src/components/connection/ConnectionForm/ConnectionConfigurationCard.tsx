import { FormattedMessage } from "react-intl";

import { CollapsibleCard } from "components/ui/CollapsibleCard";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { ConnectionConfigurationPreview } from "./ConnectionConfigurationPreview";
import { DestinationStreamPrefixNameFormField } from "./DestinationStreamPrefixNameFormField";
import { NamespaceDefinitionFormField } from "./NamespaceDefinitionFormField";
import { NonBreakingChangesPreferenceFormField } from "./NonBreakingChangesPreferenceFormField";
import { ScheduleFormField } from "./ScheduleFormField/ScheduleFormField";

export const ConnectionConfigurationCard = () => {
  const { mode } = useConnectionFormService();
  const isEditMode = mode === "edit";

  return (
    <CollapsibleCard
      title={<FormattedMessage id="form.configuration" />}
      collapsible={isEditMode}
      defaultCollapsedState={isEditMode}
      collapsedPreviewInfo={<ConnectionConfigurationPreview />}
      testId="configuration"
    >
      <ScheduleFormField />
      <NamespaceDefinitionFormField />
      <DestinationStreamPrefixNameFormField />
      <NonBreakingChangesPreferenceFormField />
    </CollapsibleCard>
  );
};
