import { zodResolver } from "@hookform/resolvers/zod";
import { useState } from "react";
import { FormProvider, useForm, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";

import LoadingSchema from "components/LoadingSchema";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink, Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useGetDestinationFromSearchParams, useGetSourceFromSearchParams } from "area/connector/utils";
import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useDestinationDefinitionList, useDiscoverSchemaQuery } from "core/api";
import { links } from "core/utils/links";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./MapFieldsRoute.module.scss";
import {
  EMPTY_STREAM,
  StreamMappingsFormValues,
  StreamMappings,
  StreamMappingsFormValuesSchema,
} from "./StreamMappings";
import { SOURCE_ID_PARAM } from "../CreateConnection/DefineSource";
import { CreateConnectionFlowLayout } from "../CreateConnectionFlowLayout";

export const MapFieldsRoute = () => {
  const navigate = useNavigate();
  const source = useGetSourceFromSearchParams();
  const destination = useGetDestinationFromSearchParams();
  const createLink = useCurrentWorkspaceLink();
  const location = useLocation();
  const [searchParams] = useSearchParams();
  const { data: sourceSchema } = useDiscoverSchemaQuery(source.sourceId);
  const [showGlobalValidationMessage, setShowGlobalValidationMessage] = useState(false);

  if (!source || !destination) {
    throw new Error("Source ID and Destination ID are required");
  }

  const methods = useForm<StreamMappingsFormValues>({
    defaultValues: {
      streams: location.state?.streams || [EMPTY_STREAM],
    },
    mode: "onChange",
    resolver: zodResolver(StreamMappingsFormValuesSchema),
  });

  const onSubmit = (values: StreamMappingsFormValues) => {
    navigate(`${ConnectionRoutePaths.ConfigureContinued}?${searchParams.toString()}`, {
      state: {
        streams: values.streams,
      },
    });
  };

  const { destinationDefinitionMap } = useDestinationDefinitionList();
  const destinationDefinition = destinationDefinitionMap.get(destination.destinationDefinitionId);
  if (!destinationDefinition) {
    throw new Error("Destination definition not found");
  }

  return (
    <FormProvider {...methods}>
      <form
        onSubmit={(event) => {
          setShowGlobalValidationMessage(true);
          return methods.handleSubmit(onSubmit)(event);
        }}
        className={styles.form}
      >
        <CreateConnectionFlowLayout.Main>
          <Box p="xl">
            <FlexContainer direction="column" gap="lg">
              <FlexContainer direction="column" gap="lg">
                <Heading as="h1">
                  <FormattedMessage id="connections.mappings.title" />
                </Heading>
                <FlexContainer justifyContent="space-between">
                  <Text>
                    <FormattedMessage
                      id="connection.dataActivationDescription"
                      values={{ destinationName: destinationDefinition.name }}
                    />
                  </Text>
                  <div style={{ flexShrink: 0 }}>
                    <Text>
                      <ExternalLink href={links.dataActivationDocs}>
                        <FormattedMessage id="connections.mappings.docsLink" /> <Icon type="share" size="xs" />
                      </ExternalLink>
                    </Text>
                  </div>
                </FlexContainer>
              </FlexContainer>
              {!sourceSchema && <LoadingSchema />}

              {sourceSchema && (
                <FlexContainer direction="column" gap="lg">
                  <StreamMappings />
                </FlexContainer>
              )}
            </FlexContainer>
          </Box>
        </CreateConnectionFlowLayout.Main>
        {sourceSchema && (
          <CreateConnectionFlowLayout.Footer>
            <Link
              to={{
                pathname: createLink(`/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`),
                search: `?${SOURCE_ID_PARAM}=${source.sourceId}`,
              }}
              variant="button"
            >
              <FormattedMessage id="connectionForm.backToDefineDestination" />
            </Link>

            <FlexContainer justifyContent="flex-end" alignItems="center">
              {showGlobalValidationMessage && <GlobalValidationMessage />}
              <Button type="submit">
                <FormattedMessage id="connectionForm.configureConnection" />
              </Button>
            </FlexContainer>
          </CreateConnectionFlowLayout.Footer>
        )}
      </form>
    </FormProvider>
  );
};

const GlobalValidationMessage = () => {
  const { formState } = useFormContext<StreamMappingsFormValues>();
  const { isValid } = formState;

  if (isValid) {
    return null;
  }

  return (
    <Text color="red">
      <FormattedMessage id="connectionForm.validation.creationError" />
    </Text>
  );
};
