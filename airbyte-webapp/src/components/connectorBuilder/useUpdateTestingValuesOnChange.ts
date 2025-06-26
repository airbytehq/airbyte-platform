import cloneDeep from "lodash/cloneDeep";
import debounce from "lodash/debounce";
import isEqual from "lodash/isEqual";
import { useEffect, useMemo, useRef, useState } from "react";
import { useFormContext } from "react-hook-form";

import { useBuilderProjectUpdateTestingValues } from "core/api";
import { ConnectorBuilderProjectTestingValues } from "core/api/types/AirbyteClient";
import { Spec } from "core/api/types/ConnectorManifest";
import { jsonSchemaToFormBlock } from "core/form/schemaToFormBlock";
import { FormGroupItem } from "core/form/types";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";
import { setDefaultValues } from "views/Connector/ConnectorForm/useBuildForm";

import { useBuilderWatch } from "./useBuilderWatch";

/**
 * Handles persisting testing values to the backend whenever they change.
 *
 * This hook uses debouncing and refs to achieve the following flow:
 *   1. testingValues change in the form
 *   2. the testingValuesDirty flag is set to true
 *   3. a call to updateTestingValues is debounced
 *   4. the testingValuesRef is updated to hold the new testingValues
 *   5. the updateTestingValues call eventually finishes, and sets the
 *      testingValuesDirty flag based on whether testingValuesRef is now
 *      equal to the testingValues value. This is so that if there are any
 *      more changes to testingValues made while the updateTestingValues
 *      call was still in progress, the testingValuesDirty flag will stay
 *      true through the next call.
 *
 * This hook also handles the case in YAML mode where the spec is changed
 * and new defaults need to be applied to the testing values. This only
 * needs to happen in YAML mode, because in UI mode we properly apply testing
 * values whenever the user input settings form is submitted (in InputsForm).
 *
 * @returns the testingValuesDirty flag
 */
export const useUpdateTestingValuesOnChange = () => {
  const { projectId } = useConnectorBuilderFormState();
  const mode = useBuilderWatch("mode");
  const { setValue, getValues } = useFormContext();
  const testingValues = useBuilderWatch("testingValues");
  const spec = useBuilderWatch("manifest.spec");
  const specRef = useRef<Spec | undefined>(spec);
  const testingValuesRef = useRef<ConnectorBuilderProjectTestingValues | undefined>(testingValues);

  const [testingValuesDirty, setTestingValuesDirty] = useState(false);
  const { mutateAsync: updateTestingValues, isLoading: testingValuesUpdating } = useBuilderProjectUpdateTestingValues(
    projectId,
    () => setTestingValuesDirty(!isEqual(testingValuesRef.current, getValues("testingValues")))
  );

  // apply default values on spec change in YAML mode (in UI mode this is handled by InputsForm)
  useEffect(() => {
    if (mode === "yaml" && !isEqual(specRef.current?.connection_specification, spec?.connection_specification)) {
      // clone testingValues because applyTestingValuesDefaults mutates the object
      const testingValuesWithDefaults = applyTestingValuesDefaults(cloneDeep(testingValues), spec);
      if (!isEqual(testingValues, testingValuesWithDefaults)) {
        setValue("testingValues", testingValuesWithDefaults);
      }
    }
    specRef.current = spec;
  }, [mode, setValue, spec, testingValues, updateTestingValues]);

  const debouncedUpdateTestingValues = useMemo(() => debounce(updateTestingValues, 500), [updateTestingValues]);

  // persist testing values on change (debounced)
  useEffect(() => {
    if (!testingValuesUpdating && !isEqual(testingValuesRef.current, testingValues)) {
      setTestingValuesDirty(true);
      debouncedUpdateTestingValues({
        spec: spec?.connection_specification ?? {},
        testingValues: testingValues ?? {},
      });
      testingValuesRef.current = testingValues;
    }
  }, [debouncedUpdateTestingValues, spec?.connection_specification, testingValues, testingValuesUpdating]);

  return { testingValuesDirty };
};

const EMPTY_SCHEMA = {};
export function applyTestingValuesDefaults(
  testingValues: ConnectorBuilderProjectTestingValues | undefined,
  spec?: Spec
) {
  const testingValuesToUpdate = testingValues || {};
  try {
    const jsonSchema = spec && spec.connection_specification ? spec.connection_specification : EMPTY_SCHEMA;
    const formFields = jsonSchemaToFormBlock(jsonSchema);
    setDefaultValues(formFields as FormGroupItem, testingValuesToUpdate, { respectExistingValues: true });
  } catch {
    // spec is user supplied so it might not be valid - prevent crashing the application by just skipping trying to set default values
  }

  return testingValues === undefined && Object.keys(testingValuesToUpdate).length === 0
    ? undefined
    : testingValuesToUpdate;
}
