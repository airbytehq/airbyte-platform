import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { PageContainer } from "components/PageContainer";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useSourceDefinitionList, useSourceList } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import { CreateNewSource, SOURCE_DEFINITION_PARAM } from "./CreateNewSource";
import { RadioButtonTiles } from "./RadioButtonTiles";
import { SelectExistingConnector } from "./SelectExistingConnector";

export type SourceType = "existing" | "new";

export const EXISTING_SOURCE_TYPE = "existing";
export const NEW_SOURCE_TYPE = "new";
export const SOURCE_TYPE_PARAM = "sourceType";
export const SOURCE_ID_PARAM = "sourceId";

export const DefineSource: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_NEW_DEFINE_SOURCE);
  const { formatMessage } = useIntl();
  const { sources } = useSourceList();
  const { sourceDefinitionMap } = useSourceDefinitionList();
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

  const sortedSources = useMemo(() => {
    return sources
      .map((source) => ({
        ...source,
        sourceDefinitionName: sourceDefinitionMap.get(source.sourceDefinitionId)?.name ?? "",
      }))
      .sort((a, b) => a.sourceDefinitionName.localeCompare(b.sourceDefinitionName) || a.name.localeCompare(b.name));
  }, [sources, sourceDefinitionMap]);

  return (
    <Box p="xl">
      <FlexContainer direction="column">
        {!searchParams.get(SOURCE_DEFINITION_PARAM) && (
          <PageContainer centered>
            <Card>
              <Heading as="h2">
                <FormattedMessage id="connectionForm.defineSource" />
              </Heading>
              <Box mt="md">
                <RadioButtonTiles
                  name="sourceType"
                  options={[
                    {
                      value: EXISTING_SOURCE_TYPE,
                      label: formatMessage({ id: "connectionForm.sourceExisting" }),
                      description: formatMessage({ id: "connectionForm.sourceExistingDescription" }),
                      disabled: sources.length === 0,
                    },
                    {
                      value: NEW_SOURCE_TYPE,
                      label: formatMessage({ id: "onboarding.sourceSetUp" }),
                      description: formatMessage({ id: "onboarding.sourceSetUp.description" }),
                    },
                  ]}
                  selectedValue={selectedSourceType}
                  onSelectRadioButton={(id) => selectSourceType(id)}
                />
              </Box>
            </Card>
          </PageContainer>
        )}
        {selectedSourceType === EXISTING_SOURCE_TYPE && (
          <PageContainer centered>
            <SelectExistingConnector connectors={sortedSources} selectConnector={selectSource} />
          </PageContainer>
        )}
        {selectedSourceType === NEW_SOURCE_TYPE && <CreateNewSource />}
        <CloudInviteUsersHint connectorType="source" />
      </FlexContainer>
    </Box>
  );
};
