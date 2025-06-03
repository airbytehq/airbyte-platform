import React, { useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { ControlLabels } from "components";
import { ConnectorIcon } from "components/ConnectorIcon";
import { HeadTitle } from "components/HeadTitle";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { ComboBox, Option, OptionSection } from "components/ui/ComboBox";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { useListBuilderProjects, useListBuilderProjectVersions } from "core/api";
import { ConnectorBuilderLocalStorageProvider } from "services/connectorBuilder__deprecated/ConnectorBuilderLocalStorageService";

import styles from "./ConnectorBuilderForkPage.module.scss";
import { AirbyteTitle } from "../components/AirbyteTitle";
import { BackButton } from "../components/BackButton";
import { useBuilderCompatibleSourceDefinitions } from "../components/useBuilderCompatibleSourceDefinitions";
import { useCreateAndNavigate } from "../components/useCreateAndNavigate";
import { ConnectorBuilderRoutePaths } from "../ConnectorBuilderRoutes";

const ConnectorBuilderForkPageInner: React.FC = () => {
  const { formatMessage } = useIntl();

  const projects = useListBuilderProjects();
  const { builderCompatibleSourceDefinitions, sourceDefinitionMap } = useBuilderCompatibleSourceDefinitions();

  const { createAndNavigate, forkAndNavigate, isLoading: isCreating } = useCreateAndNavigate();
  const navigate = useNavigate();

  useEffect(() => {
    if (projects.length === 0 && builderCompatibleSourceDefinitions.length === 0) {
      navigate(ConnectorBuilderRoutePaths.Create, { replace: true });
    }
  }, [builderCompatibleSourceDefinitions.length, navigate, projects.length]);

  const [selectedId, setSelectedId] = useState<string | undefined>(undefined);
  const selection = useMemo(() => {
    if (!selectedId) {
      return undefined;
    }

    const sourceDefinition = sourceDefinitionMap.get(selectedId);
    if (sourceDefinition) {
      return { type: "sourceDefinition", sourceDefinition } as const;
    }

    const selectedProject = projects.find((project) => project.id === selectedId);
    if (selectedProject) {
      return { type: "builderProject", project: selectedProject } as const;
    }

    return undefined;
  }, [projects, selectedId, sourceDefinitionMap]);

  const connectorOptions: OptionSection[] = useMemo(() => {
    const builderProjectOptions: Option[] = projects.map((project) => {
      return { label: project.name, value: project.id };
    });

    const sourceDefinitionOptions: Option[] = builderCompatibleSourceDefinitions.map((sourceDefinition) => {
      return {
        label: sourceDefinition.name,
        value: sourceDefinition.sourceDefinitionId,
        iconLeft: <ConnectorIcon icon={sourceDefinition.icon} className={styles.connectorIcon} />,
      };
    });

    return [
      { sectionTitle: "AIRBYTE", innerOptions: sourceDefinitionOptions },
      { sectionTitle: "Custom", innerOptions: builderProjectOptions },
    ];
  }, [builderCompatibleSourceDefinitions, projects]);

  const selectedProject = useMemo(
    () => (selection && selection.type === "builderProject" ? selection.project : undefined),
    [selection]
  );
  const { data: versions, isLoading: isLoadingVersions } = useListBuilderProjectVersions(selectedProject);
  const [selectedBuilderProjectVersion, setSelectedBuilderProjectVersion] = useState<VersionOption["value"]>("draft");
  useEffect(() => {
    if (!versions) {
      return;
    }
    setSelectedBuilderProjectVersion(selectedProject?.hasDraft ? "draft" : versions[0]?.version);
  }, [selectedProject, versions]);

  const versionOptions: VersionOption[] = useMemo(() => {
    const options: VersionOption[] = (versions || []).map(({ version, description }) => {
      return {
        label: (
          <FlexContainer alignItems="baseline">
            <Text size="md" as="span">
              v{version}{" "}
            </Text>
            <Text size="sm" as="span" color="grey">
              {description}
            </Text>
          </FlexContainer>
        ),
        value: version,
      };
    });
    if (selectedProject?.hasDraft) {
      options.unshift({
        label: (
          <Text size="md" as="span">
            <FormattedMessage id="connectorBuilder.draft" />
          </Text>
        ),
        value: "draft",
      });
    }
    return options;
  }, [selectedProject, versions]);

  const isLoading = isCreating || isLoadingVersions;

  return (
    <FlexContainer direction="column" gap="2xl" className={styles.container}>
      <FlexContainer direction="column" alignItems="center" gap="md">
        <AirbyteTitle title={<FormattedMessage id="connectorBuilder.forkPage.prompt" />} />
        <Text color="grey500" size="sm">
          <FormattedMessage id="connectorBuilder.forkPage.description" />
        </Text>
      </FlexContainer>
      <FlexContainer direction="column" gap="xl">
        <Card className={styles.form} noPadding>
          <FlexContainer direction="column" gap="lg">
            <ControlLabels label={formatMessage({ id: "connectorBuilder.forkPage.search" })}>
              <ComboBox
                options={connectorOptions}
                onChange={setSelectedId}
                value={selectedId}
                filterOptions
                allowCustomValue={false}
                data-testid="fork-connector-combobox"
              />
            </ControlLabels>
            {versionOptions.length > 1 && (
              <ControlLabels label={formatMessage({ id: "connectorBuilder.forkPage.version" })}>
                <ListBox<"draft" | number>
                  options={versionOptions}
                  onSelect={(selected) => selected && setSelectedBuilderProjectVersion(selected)}
                  selectedValue={selectedBuilderProjectVersion}
                />
              </ControlLabels>
            )}
            {selection && (
              <Text color="grey500">
                {selection.type === "builderProject" ? (
                  <FormattedMessage
                    id="connectorBuilder.forkPage.connectorSelectionText.builderProject"
                    values={{
                      name: selection.project.name,
                      version:
                        selectedBuilderProjectVersion === "draft"
                          ? "current draft"
                          : `v${selectedBuilderProjectVersion}`,
                    }}
                  />
                ) : (
                  <FormattedMessage
                    id="connectorBuilder.forkPage.connectorSelectionText.catalogConnector"
                    values={{
                      name: selection.sourceDefinition.name,
                    }}
                  />
                )}
              </Text>
            )}
          </FlexContainer>
        </Card>
        <FlexContainer direction="row-reverse">
          <Button
            disabled={isLoading || !selection}
            isLoading={isLoading}
            onClick={() => {
              if (isLoading || !selection) {
                return;
              }
              if (selection.type === "sourceDefinition") {
                forkAndNavigate(selection.sourceDefinition.sourceDefinitionId);
                return;
              }
              if (selection.type === "builderProject") {
                createAndNavigate({
                  name: formatMessage(
                    { id: "connectorBuilder.forkPage.copyName" },
                    { oldName: selection.project.name }
                  ),
                  forkProjectId: selection.project.id,
                  version: selectedBuilderProjectVersion,
                });
              }
            }}
          >
            <FormattedMessage id="connectorBuilder.forkPage.createLabel" />
          </Button>
        </FlexContainer>
      </FlexContainer>
    </FlexContainer>
  );
};

export const ConnectorBuilderForkPage: React.FC = () => (
  <ConnectorBuilderLocalStorageProvider>
    <HeadTitle titles={[{ id: "connectorBuilder.title" }]} />
    <BackButton />
    <ConnectorBuilderForkPageInner />
  </ConnectorBuilderLocalStorageProvider>
);

interface VersionOption {
  label: React.ReactNode;
  value: "draft" | number;
}
