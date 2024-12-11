import { PageContainer } from "components/PageContainer";
import { ScrollParent } from "components/ui/ScrollParent";

import { FeatureItem, IfFeatureDisabled, IfFeatureEnabled } from "core/services/features";

import { ConnectionMappingsList } from "./ConnectionMappingsList";
import { MappingContextProvider, useMappingContext } from "./MappingContext";
import { MappingsEmptyState } from "./MappingsEmptyState";
import { MappingsUpsellEmptyState } from "./MappingsUpsellEmptyState";

export const ConnectionMappingsPage = () => {
  return (
    <ScrollParent>
      <PageContainer centered>
        <MappingContextProvider>
          <ConnectionMappingsPageContent />
        </MappingContextProvider>
      </PageContainer>
    </ScrollParent>
  );
};

const ConnectionMappingsPageContent = () => {
  const { streamsWithMappings } = useMappingContext();
  return (
    <>
      <IfFeatureEnabled feature={FeatureItem.MappingsUI}>
        {Object.entries(streamsWithMappings).length > 0 ? <ConnectionMappingsList /> : <MappingsEmptyState />}
      </IfFeatureEnabled>
      <IfFeatureDisabled feature={FeatureItem.MappingsUI}>
        <MappingsUpsellEmptyState />
      </IfFeatureDisabled>
    </>
  );
};
