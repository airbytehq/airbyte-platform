import get from "lodash/get";
import { useCallback, useMemo } from "react";
import { FieldPath, useFormContext } from "react-hook-form";

import { ConnectorSpecification } from "core/domain/connector";
import { isSourceDefinitionSpecificationDraft } from "core/domain/connector/source";
import { FeatureItem, useFeature } from "core/services/features";
import { trackError } from "core/utils/datadog";

import { useConnectorForm } from "./connectorFormContext";
import { ConnectorFormValues } from "./types";
import { authPredicateMatchesPath, makeConnectionConfigurationPath, serverProvidedOauthPaths } from "./utils";

/**
 * Returns true if the auth button should be shown for an advancedAuth specification.
 * This will check if the connector has a predicateKey, and if so, check if the current form value
 * of the corresponding field matches the predicateValue from the specification.
 */
const shouldShowButtonForAdvancedAuth = (
  predicateKey: string[] | undefined,
  predicateValue: string | undefined,
  values: ConnectorFormValues<unknown>
): boolean => {
  return (
    !predicateKey ||
    predicateKey.length === 0 ||
    predicateValue === get(values, makeConnectionConfigurationPath(predicateKey))
  );
};

interface AuthenticationHook {
  /**
   * Returns whether a given field path should be hidden, because it's part of the
   * OAuth flow and will be filled in by that.
   */
  isHiddenAuthField: (fieldPath: string) => boolean;
  /**
   * A record of all formik errors in hidden authentication fields. The key will be the
   * name of the field and the value an error code. If no error is present in a field
   * it will be missing from this object.
   */
  hiddenAuthFieldErrors: Record<string, string>;
  /**
   * This will return true if the auth button should be visible and rendered in the place of
   * the passed in field, and false otherwise.
   */
  shouldShowAuthButton: (fieldPath: string) => boolean;
  /**
   * This will return true if any of the hidden auth fields have values.  This determines
   * whether we will render "authenticate" or "re-authenticate" on the OAuth button text
   */
  hasAuthFieldValues: boolean;
  /**
   * This will return true if Airbyte global credentials are unavailable for the connector.
   * This is used to determine whether we should render an info tooltip for the user to
   * tell them what the redirect URL of their OAuth app should be.
   */
  shouldShowRedirectUrlTooltip: boolean;
}

export const useAuthentication = (): AuthenticationHook => {
  const {
    watch,
    getFieldState,
    formState: { submitCount },
  } = useFormContext<ConnectorFormValues>();
  const values = watch();
  const { getValues, selectedConnectorDefinitionSpecification: connectorSpec } = useConnectorForm();

  const allowOAuthConnector = useFeature(FeatureItem.AllowOAuthConnector);

  const advancedAuth = connectorSpec?.advancedAuth;

  const getValuesSafe = useCallback(
    (values: ConnectorFormValues<unknown>) => {
      try {
        // We still see cases where calling `getValues` which will eventually use the yupSchema.cast method
        // crashes based on the passed in values. To temporarily make sure we're not crashing the form, we're
        // falling back to `values` in case the cast fails. This is a temporary patch, and we need to investigate
        // all the failures happening here properly.
        return getValues(values);
      } catch (e) {
        console.error(`getValues in useAuthentication failed.`, e);
        trackError(e, {
          id: "useAuthentication.getValues",
          connector:
            connectorSpec && !isSourceDefinitionSpecificationDraft(connectorSpec)
              ? ConnectorSpecification.id(connectorSpec)
              : null,
        });
        return values;
      }
    },
    [connectorSpec, getValues]
  );

  const valuesWithDefaults = useMemo(() => getValuesSafe(values), [getValuesSafe, values]);

  const isAuthButtonVisible = useMemo(
    () =>
      Boolean(
        allowOAuthConnector &&
          advancedAuth &&
          shouldShowButtonForAdvancedAuth(advancedAuth.predicateKey, advancedAuth.predicateValue, valuesWithDefaults)
      ),
    [advancedAuth, valuesWithDefaults, allowOAuthConnector]
  );

  const shouldShowRedirectUrlTooltip = connectorSpec?.advancedAuthGlobalCredentialsAvailable === false;

  // Fields that are filled by the OAuth flow and thus won't need to be shown in the UI if OAuth is available
  const implicitAuthFieldPaths = useMemo(
    () =>
      advancedAuth && !isSourceDefinitionSpecificationDraft(connectorSpec)
        ? Object.values(serverProvidedOauthPaths(connectorSpec)).map((f) =>
            makeConnectionConfigurationPath(f.path_in_connector_config)
          )
        : [],
    [advancedAuth, connectorSpec]
  );

  const isHiddenAuthField = useCallback(
    (fieldPath: string) => {
      // A field should be hidden due to OAuth if we have OAuth enabled and selected (in case it's inside a oneOf)
      // and the field is part of the OAuth flow parameters.
      return isAuthButtonVisible && implicitAuthFieldPaths.includes(fieldPath);
    },
    [implicitAuthFieldPaths, isAuthButtonVisible]
  );

  const hiddenAuthFieldErrors = useMemo(() => {
    if (!isAuthButtonVisible) {
      // We don't want to return the errors if the auth button isn't visible.
      return {};
    }
    return implicitAuthFieldPaths.reduce<Record<string, string>>((authErrors, fieldName) => {
      const { error } = getFieldState(fieldName as FieldPath<ConnectorFormValues>);
      if (submitCount > 0 && error && error.type) {
        authErrors[fieldName] = error.type;
      }
      return authErrors;
    }, {});
  }, [getFieldState, implicitAuthFieldPaths, isAuthButtonVisible, submitCount]);

  const shouldShowAuthButton = useCallback(
    (fieldPath: string) => {
      if (!isAuthButtonVisible) {
        // Never show the auth button anywhere if its not enabled or visible (inside a conditional that's not selected)
        return false;
      }

      return authPredicateMatchesPath(fieldPath, connectorSpec);
    },
    [connectorSpec, isAuthButtonVisible]
  );

  const hasAuthFieldValues: boolean = useMemo(() => {
    return implicitAuthFieldPaths.some((path) => !!get(valuesWithDefaults, path));
  }, [implicitAuthFieldPaths, valuesWithDefaults]);

  return {
    isHiddenAuthField,
    hiddenAuthFieldErrors,
    shouldShowAuthButton,
    hasAuthFieldValues,
    shouldShowRedirectUrlTooltip,
  };
};
