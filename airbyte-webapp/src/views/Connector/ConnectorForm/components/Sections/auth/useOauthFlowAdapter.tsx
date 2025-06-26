import get from "lodash/get";
import isEmpty from "lodash/isEmpty";
import pick from "lodash/pick";
import set from "lodash/set";
import { useState } from "react";
import { FieldPath, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { CompleteOAuthResponseAuthPayload } from "core/api/types/AirbyteClient";
import { ConnectorDefinitionSpecificationRead } from "core/domain/connector";
import { useRunOauthFlow, useRunOauthFlowBuilder } from "hooks/services/useConnectorAuth";
import { useAuthentication } from "views/Connector/ConnectorForm/useAuthentication";

import { useNotificationService } from "../../../../../../hooks/services/Notification";
import { useConnectorForm } from "../../../connectorFormContext";
import { ConnectorFormValues } from "../../../types";
import { makeConnectionConfigurationPath, serverProvidedOauthPaths, userProvidedOauthInputPaths } from "../../../utils";

export function useFormOauthAdapter(connector: ConnectorDefinitionSpecificationRead): {
  loading: boolean;
  done?: boolean;
  hasRun: boolean;
  run: () => Promise<void>;
} {
  const { setValue, getValues: getRawValues, formState } = useFormContext<ConnectorFormValues>();
  const [hasRun, setHasRun] = useState(false);

  const { getValues } = useConnectorForm();
  const { registerNotification } = useNotificationService();

  const OAUTH_SUCCESS_ID = "connectorForm.authenticate.succeeded";

  const onDone = (authPayload: CompleteOAuthResponseAuthPayload) => {
    const oauthPaths = serverProvidedOauthPaths(connector);

    const newValues = Object.entries(oauthPaths).reduce(
      (acc, [key, { path_in_connector_config }]) =>
        set(acc, makeConnectionConfigurationPath(path_in_connector_config), authPayload[key]),
      getRawValues()
    );

    Object.entries(newValues).forEach(([key, value]) => {
      setValue(key as keyof ConnectorFormValues, value, {
        shouldDirty: true,
        shouldTouch: true,
        // do not validate, otherwise all unfilled fields will be marked as invalid
        // in the off-chance something is wrong with the oauth values, it will be flagged when the user tries to submit the form
        shouldValidate: false,
      });
    });

    registerNotification({
      id: OAUTH_SUCCESS_ID,
      text: <FormattedMessage id={OAUTH_SUCCESS_ID} />,
      type: "success",
    });

    setHasRun(true);
  };

  const { run, loading, done } = useRunOauthFlow({ connector, onDone });

  const { hasAuthFieldValues } = useAuthentication();

  return {
    loading,
    done: done || hasAuthFieldValues,
    hasRun,
    run: async () => {
      const oauthInputProperties = userProvidedOauthInputPaths(connector);

      if (!isEmpty(oauthInputProperties)) {
        const oauthInputFields =
          Object.values(oauthInputProperties)?.map((property) =>
            makeConnectionConfigurationPath(property.path_in_connector_config)
          ) ?? [];

        oauthInputFields.forEach((path) =>
          setValue(path as FieldPath<ConnectorFormValues>, getRawValues(path as FieldPath<ConnectorFormValues>), {
            shouldDirty: true,
            shouldTouch: true,
            shouldValidate: true,
          })
        );

        const oAuthErrors = pick(formState.errors, oauthInputFields);

        if (!isEmpty(oAuthErrors)) {
          return;
        }
      }

      const preparedValues = getValues(getRawValues());
      const oauthInputParams = Object.entries(oauthInputProperties).reduce(
        (acc, property) => {
          acc[property[0]] = get(preparedValues, makeConnectionConfigurationPath(property[1].path_in_connector_config));
          return acc;
        },
        {} as Record<string, unknown>
      );

      run(oauthInputParams);
    },
  };
}

export function useFormOauthAdapterBuilder(
  builderProjectId: string,
  onComplete: (authPayload: Record<string, unknown>) => void
): {
  loading: boolean;
  done?: boolean;
  hasRun: boolean;
  run: () => Promise<void>;
} {
  // const { setValue, getValues: getRawValues, formState } = useFormContext<ConnectorFormValues<Credentials>>();
  const [hasRun, setHasRun] = useState(false);

  // const { getValues } = useConnectorForm();
  const { registerNotification } = useNotificationService();

  const OAUTH_SUCCESS_ID = "connectorForm.authenticate.succeeded";

  const onDone = (authPayload: CompleteOAuthResponseAuthPayload) => {
    onComplete(authPayload);
    // const oauthPaths = serverProvidedOauthPaths(connector);

    // const newValues = Object.entries(oauthPaths).reduce(
    //   (acc, [key, { path_in_connector_config }]) =>
    //     set(acc, makeConnectionConfigurationPath(path_in_connector_config), authPayload[key]),
    //   getRawValues()
    // );

    // Object.entries(newValues).forEach(([key, value]) => {
    //   setValue(key as keyof ConnectorFormValues<Credentials>, value, {
    //     shouldDirty: true,
    //     shouldTouch: true,
    //     // do not validate, otherwise all unfilled fields will be marked as invalid
    //     // in the off-chance something is wrong with the oauth values, it will be flagged when the user tries to submit the form
    //     shouldValidate: false,
    //   });
    // });

    registerNotification({
      id: OAUTH_SUCCESS_ID,
      text: <FormattedMessage id={OAUTH_SUCCESS_ID} />,
      type: "success",
    });

    setHasRun(true);
  };

  const { run, loading, done } = useRunOauthFlowBuilder({ builderProjectId, onDone });

  return {
    loading,
    done,
    hasRun,
    run: async () => {
      // const oauthInputProperties =
      //   (
      //     connector?.advancedAuth?.oauthConfigSpecification?.oauthUserInputFromConnectorConfigSpecification as {
      //       properties: Array<{ path_in_connector_config: string[] }>;
      //     }
      //   )?.properties ?? {};

      // if (!isEmpty(oauthInputProperties)) {
      //   const oauthInputFields =
      //     Object.values(oauthInputProperties)?.map((property) =>
      //       makeConnectionConfigurationPath(property.path_in_connector_config)
      //     ) ?? [];

      //   oauthInputFields.forEach((path) =>
      //     setValue(
      //       path as FieldPath<ConnectorFormValues<Credentials>>,
      //       getRawValues(path as FieldPath<ConnectorFormValues<Credentials>>),
      //       {
      //         shouldDirty: true,
      //         shouldTouch: true,
      //         shouldValidate: true,
      //       }
      //     )
      //   );

      //   const oAuthErrors = pick(formState.errors, oauthInputFields);

      //   if (!isEmpty(oAuthErrors)) {
      //     return;
      //   }
      // }

      // const preparedValues = getValues<Credentials>(getRawValues());
      // const oauthInputParams = Object.entries(oauthInputProperties).reduce(
      //   (acc, property) => {
      //     acc[property[0]] = get(preparedValues, makeConnectionConfigurationPath(property[1].path_in_connector_config));
      //     return acc;
      //   },
      //   {} as Record<string, unknown>
      // );

      const oauthInputParams = {};

      run(oauthInputParams);
    },
  };
}
