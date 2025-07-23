import { useCreatePartialUserConfig, useGetConfigTemplate } from "core/api";
import { AdvancedAuth, SourceDefinitionSpecification } from "core/api/types/AirbyteClient";
import { IsAirbyteEmbeddedContext } from "core/services/embedded";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigForm } from "./PartialUserConfigForm";
import { PartialUserConfigSuccessView } from "./PartialUserConfigSuccessView";
import { useEmbeddedSourceParams } from "../hooks/useEmbeddedSourceParams";

export const PartialUserConfigCreateForm: React.FC = () => {
  const { workspaceId, selectedTemplateId } = useEmbeddedSourceParams();
  const { mutate: createPartialUserConfig, isSuccess } = useCreatePartialUserConfig();
  const configTemplate = useGetConfigTemplate(selectedTemplateId ?? "");

  const advanced_auth = configTemplate.user_config_spec.advanced_auth;

  const sourceDefinitionSpecification: SourceDefinitionSpecification = {
    ...configTemplate.user_config_spec,

    sourceDefinitionId: configTemplate.source_definition_id,
    ...(advanced_auth && {
      advancedAuth: {
        authFlowType:
          advanced_auth.auth_flow_type === "oauth2.0"
            ? "oauth2.0"
            : advanced_auth.auth_flow_type === "oauth1.0"
            ? "oauth1.0"
            : "",
        predicateKey: advanced_auth.predicate_key,
        predicateValue: advanced_auth.predicate_value,
        oauthConfigSpecification: {
          oauthUserInputFromConnectorConfigSpecification:
            advanced_auth.oauth_config_specification?.oauth_user_input_from_connector_config_specification ?? null,
          completeOAuthOutputSpecification:
            advanced_auth.oauth_config_specification?.complete_oauth_output_specification ?? null,
          completeOAuthServerInputSpecification:
            advanced_auth.oauth_config_specification?.complete_oauth_server_input_specification ?? null,
          completeOAuthServerOutputSpecification:
            advanced_auth.oauth_config_specification?.complete_oauth_server_output_specification ?? null,
        },
      } as AdvancedAuth,
      advancedAuthGlobalCredentialsAvailable: Boolean(advanced_auth),
    }),
  };

  const onSubmit = (values: ConnectorFormValues) => {
    return new Promise<void>((resolve, reject) => {
      createPartialUserConfig(
        {
          workspace_id: workspaceId,
          source_config_template_id: selectedTemplateId ?? "",
          connection_configuration: values.connectionConfiguration,
        },
        {
          onSuccess: () => resolve(),
          onError: (error) => reject(error),
        }
      );
    });
  };

  if (isSuccess) {
    return (
      <PartialUserConfigSuccessView
        successType="create"
        connectorName={configTemplate.name}
        icon={configTemplate.icon ?? ""}
      />
    );
  }

  return (
    <IsAirbyteEmbeddedContext.Provider value>
      <PartialUserConfigForm
        isEditMode={false}
        connectorName={configTemplate.name}
        icon={configTemplate.icon ?? ""}
        onSubmit={onSubmit}
        sourceDefinitionSpecification={sourceDefinitionSpecification}
      />
    </IsAirbyteEmbeddedContext.Provider>
  );
};
