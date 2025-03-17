import { useState } from "react";
import { useSearchParams } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { useCreateActorConnectionMask, useGetActorConnectionTemplate } from "core/api";
import { ActorMaskStreamConfig } from "core/api/types/AirbyteClient";
import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { Controls } from "views/Connector/ConnectorCard/components/Controls";
import { ConnectorForm, ConnectorFormValues } from "views/Connector/ConnectorForm";

import styles from "./MaskCreateForm.module.scss";

export const MaskCreateForm: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedTemplateId = searchParams.get("selectedTemplateId");
  const { mutate: createActorConnectionMask } = useCreateActorConnectionMask();
  const actorConnectionTemplate = useGetActorConnectionTemplate(selectedTemplateId ?? "");
  const maskDefinitionSpecification: SourceDefinitionSpecificationDraft = {
    connectionSpecification: actorConnectionTemplate.actorProperties.connectionSpecification,
  };

  const [streamSelectionArray, setStreamSelectionArray] = useState<ActorMaskStreamConfig[]>(
    actorConnectionTemplate.streamSelection.map((stream) => ({
      streamDescriptor: stream.streamDescriptor,
      status: stream.status === "selected" || stream.status === "suggested" ? "selected" : "deselected",
    }))
  );

  const onSubmit = (values: ConnectorFormValues) => {
    createActorConnectionMask({
      actorTemplateId: selectedTemplateId ?? "",
      streamSelection: streamSelectionArray,
      actorProperties: values,
    });
  };

  return (
    <>
      <Box py="sm">
        <Button
          variant="light"
          onClick={() => {
            setSearchParams((params) => {
              params.delete("selectedTemplateId");
              return params;
            });
          }}
        >
          Back
        </Button>
      </Box>
      <div>
        <ConnectorForm
          trackDirtyChanges
          formType="source"
          selectedConnectorDefinitionSpecification={maskDefinitionSpecification}
          onSubmit={async (values: ConnectorFormValues) => {
            onSubmit(values);
          }}
          canEdit
          renderFooter={({ dirty, isSubmitting, isValid, resetConnectorForm }) =>
            actorConnectionTemplate && (
              <>
                {streamSelectionArray.length > 0 && (
                  <Card>
                    <Text size="lg" bold>
                      Stream selection <InfoTooltip>You can optionally select which data streams to sync.</InfoTooltip>
                    </Text>
                    <ul className={styles.list}>
                      {streamSelectionArray.map((stream) => {
                        return (
                          <li key={stream.streamDescriptor.name}>
                            <Box py="sm">
                              <FlexContainer alignItems="center" gap="sm">
                                <CheckBox
                                  id={stream.streamDescriptor.name}
                                  checked={stream.status === "selected"}
                                  onChange={() => {
                                    setStreamSelectionArray(
                                      streamSelectionArray.map((s) =>
                                        s.streamDescriptor.name === stream.streamDescriptor.name &&
                                        s.streamDescriptor.namespace === stream.streamDescriptor.namespace
                                          ? { ...s, status: s.status === "selected" ? "deselected" : "selected" }
                                          : s
                                      )
                                    );
                                  }}
                                />
                                <label htmlFor={stream.streamDescriptor.name}>
                                  <Text size="md">{stream.streamDescriptor.name}</Text>
                                </label>
                              </FlexContainer>
                            </Box>
                          </li>
                        );
                      })}
                    </ul>
                  </Card>
                )}
                {/* //todo: ip allowlist banner? */}
                <Controls
                  isEditMode={false}
                  isTestConnectionInProgress={false}
                  onCancelTesting={() => {
                    return null;
                  }}
                  isSubmitting={isSubmitting}
                  formType="source"
                  hasDefinition={false}
                  onRetestClick={() => {
                    return null;
                  }}
                  onDeleteClick={() => {
                    return null;
                  }}
                  isValid={isValid}
                  dirty={dirty}
                  job={undefined}
                  onCancelClick={() => {
                    resetConnectorForm();
                  }}
                  connectionTestSuccess={false}
                />
              </>
            )
          }
        />
      </div>
    </>
  );
};
