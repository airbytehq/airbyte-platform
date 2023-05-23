import get from "lodash/get";
import isEmpty from "lodash/isEmpty";
import merge from "lodash/merge";
import pick from "lodash/pick";
import set from "lodash/set";
import { useState } from "react";
import { FieldPath, useFormContext } from "react-hook-form";

import { ConnectorDefinition, ConnectorDefinitionSpecification } from "core/domain/connector";
import { AuthSpecification, CompleteOAuthResponseAuthPayload } from "core/request/AirbyteClient";
import { useRunOauthFlow } from "hooks/services/useConnectorAuth";
import { useAuthentication } from "views/Connector/ConnectorForm/useAuthentication";

import { useConnectorForm } from "../../../connectorFormContext";
import { ConnectorFormValues } from "../../../types";
import { makeConnectionConfigurationPath, serverProvidedOauthPaths } from "../../../utils";

interface Credentials {
  credentials: AuthSpecification;
}

function useFormikOauthAdapter(
  connector: ConnectorDefinitionSpecification,
  connectorDefinition?: ConnectorDefinition
): {
  loading: boolean;
  done?: boolean;
  hasRun: boolean;
  run: () => Promise<void>;
} {
  const { setValue, getValues: getRawValues, formState } = useFormContext<ConnectorFormValues<Credentials>>();
  const [hasRun, setHasRun] = useState(false);

  const { getValues } = useConnectorForm();

  const onDone = (authPayload: CompleteOAuthResponseAuthPayload) => {
    let newValues: ConnectorFormValues<Credentials>;

    if (connector.advancedAuth) {
      const oauthPaths = serverProvidedOauthPaths(connector);

      newValues = Object.entries(oauthPaths).reduce(
        (acc, [key, { path_in_connector_config }]) =>
          set(acc, makeConnectionConfigurationPath(path_in_connector_config), authPayload[key]),
        getRawValues()
      );
    } else {
      newValues = merge({}, getRawValues(), {
        connectionConfiguration: authPayload,
      });
    }

    Object.entries(newValues).forEach(([key, value]) => {
      setValue(key as keyof ConnectorFormValues<Credentials>, value, {
        shouldDirty: true,
        shouldTouch: true,
        shouldValidate: true,
      });
    });
    setHasRun(true);
  };

  const { run, loading, done } = useRunOauthFlow({ connector, connectorDefinition, onDone });

  const { hasAuthFieldValues } = useAuthentication();

  return {
    loading,
    done: done || hasAuthFieldValues,
    hasRun,
    run: async () => {
      const oauthInputProperties =
        (
          connector?.advancedAuth?.oauthConfigSpecification?.oauthUserInputFromConnectorConfigSpecification as {
            properties: Array<{ path_in_connector_config: string[] }>;
          }
        )?.properties ?? {};

      if (!isEmpty(oauthInputProperties)) {
        const oauthInputFields =
          Object.values(oauthInputProperties)?.map((property) =>
            makeConnectionConfigurationPath(property.path_in_connector_config)
          ) ?? [];

        oauthInputFields.forEach((path) =>
          setValue(
            path as FieldPath<ConnectorFormValues<Credentials>>,
            getRawValues(path as FieldPath<ConnectorFormValues<Credentials>>),
            {
              shouldDirty: true,
              shouldTouch: true,
              shouldValidate: true,
            }
          )
        );

        const oAuthErrors = pick(formState.errors, oauthInputFields);

        if (!isEmpty(oAuthErrors)) {
          return;
        }
      }

      const preparedValues = getValues<Credentials>(getRawValues());
      const oauthInputParams = Object.entries(oauthInputProperties).reduce((acc, property) => {
        acc[property[0]] = get(preparedValues, makeConnectionConfigurationPath(property[1].path_in_connector_config));
        return acc;
      }, {} as Record<string, unknown>);

      run(oauthInputParams);
    },
  };
}

export { useFormikOauthAdapter };
