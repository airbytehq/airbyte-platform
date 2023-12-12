import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { PageContainer } from "components/PageContainer";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useConnectionList, useDestinationList } from "core/api";

import { CreateNewDestination, DESTINATION_DEFINITION_PARAM } from "./CreateNewDestination";
import { RadioButtonTiles } from "./RadioButtonTiles";
import { SelectExistingConnector } from "./SelectExistingConnector";

type DestinationType = "existing" | "new";

export const EXISTING_DESTINATION_TYPE = "existing";
export const NEW_DESTINATION_TYPE = "new";
export const DESTINATION_TYPE_PARAM = "destinationType";
export const DESTINATION_ID_PARAM = "destinationId";

export const SelectDestination: React.FC = () => {
  const { destinations } = useDestinationList();
  const connectionList = useConnectionList();
  const [searchParams, setSearchParams] = useSearchParams();

  if (!searchParams.get(DESTINATION_TYPE_PARAM)) {
    if (destinations.length === 0) {
      searchParams.set(DESTINATION_TYPE_PARAM, NEW_DESTINATION_TYPE);
      setSearchParams(searchParams);
    } else {
      searchParams.set(DESTINATION_TYPE_PARAM, EXISTING_DESTINATION_TYPE);
      setSearchParams(searchParams);
    }
  }

  const selectedDestinationType = useMemo(() => {
    return searchParams.get(DESTINATION_TYPE_PARAM) as DestinationType;
  }, [searchParams]);

  const selectDestinationType = (destinationType: DestinationType) => {
    const newParams = new URLSearchParams(searchParams);
    newParams.delete(DESTINATION_DEFINITION_PARAM);
    newParams.set(DESTINATION_TYPE_PARAM, destinationType);
    setSearchParams(newParams);
  };

  const selectDestination = (destinationId: string) => {
    const newParams = new URLSearchParams(searchParams);
    newParams.delete(DESTINATION_TYPE_PARAM);
    newParams.set(DESTINATION_ID_PARAM, destinationId);
    setSearchParams(newParams);
  };

  const sortedDestinations = useMemo(() => {
    return destinations
      .map((destination) => ({
        ...destination,
        connectionCount: connectionList?.connectionsByConnectorId.get(destination.destinationId)?.length || 0,
      }))
      .sort((a, b) => b.connectionCount - a.connectionCount || a.name.localeCompare(b.name));
  }, [destinations, connectionList?.connectionsByConnectorId]);

  return (
    <Box py="xl">
      <FlexContainer direction="column">
        {!searchParams.get(DESTINATION_DEFINITION_PARAM) && (
          <Box px="md">
            <PageContainer centered>
              <Card withPadding>
                <Heading as="h2">
                  <FormattedMessage id="connectionForm.defineDestination" />
                </Heading>
                <Box mt="md">
                  <RadioButtonTiles
                    name="destinationType"
                    options={[
                      {
                        value: EXISTING_DESTINATION_TYPE,
                        label: "connectionForm.destinationExisting",
                        description: "connectionForm.destinationExistingDescription",
                        disabled: destinations.length === 0,
                      },
                      {
                        value: NEW_DESTINATION_TYPE,
                        label: "connectionForm.destinationNew",
                        description: "connectionForm.destinationNewDescription",
                      },
                    ]}
                    selectedValue={selectedDestinationType}
                    onSelectRadioButton={(id) => selectDestinationType(id)}
                  />
                </Box>
              </Card>
            </PageContainer>
          </Box>
        )}
        <Box mt="xl">
          {selectedDestinationType === EXISTING_DESTINATION_TYPE && (
            <Box px="md">
              <PageContainer centered>
                <SelectExistingConnector connectors={sortedDestinations} selectConnector={selectDestination} />
              </PageContainer>
            </Box>
          )}
          {selectedDestinationType === NEW_DESTINATION_TYPE && <CreateNewDestination />}
        </Box>
        <CloudInviteUsersHint connectorType="destination" />
      </FlexContainer>
    </Box>
  );
};
