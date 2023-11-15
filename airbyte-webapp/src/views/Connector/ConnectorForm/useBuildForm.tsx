import { JSONSchema7Definition } from "json-schema";
import { useMemo } from "react";
import { useIntl } from "react-intl";
import { AnySchema } from "yup";

import {
  ConnectorDefinitionSpecification,
  ConnectorSpecification,
  SourceDefinitionSpecificationDraft,
} from "core/domain/connector";
import { isSourceDefinitionSpecificationDraft } from "core/domain/connector/source";
import { FormBuildError, isFormBuildError } from "core/form/FormBuildError";
import { jsonSchemaToFormBlock } from "core/form/schemaToFormBlock";
import { buildYupFormForJsonSchema } from "core/form/schemaToYup";
import { FormBlock, FormGroupItem, GroupDetails } from "core/form/types";
import { AirbyteJSONSchema } from "core/jsonSchema/types";
import { FeatureItem, useFeature } from "core/services/features";

import { ConnectorFormValues } from "./types";
import { authPredicateMatchesPath } from "./utils";

const NAME_GROUP_ID = "__name_group";

export interface BuildFormHook {
  initialValues: ConnectorFormValues;
  formFields: FormBlock[];
  validationSchema: AnySchema;
  groups: GroupDetails[];
}

export function setDefaultValues(
  formGroup: FormGroupItem,
  values: Record<string, unknown>,
  options: { respectExistingValues: boolean } = { respectExistingValues: false }
) {
  formGroup.properties.forEach((property) => {
    if (property.const && (!options.respectExistingValues || !values[property.fieldKey])) {
      values[property.fieldKey] = property.const;
    }
    if ("default" in property && (!options.respectExistingValues || !values[property.fieldKey])) {
      values[property.fieldKey] = property.default;
    }
    switch (property._type) {
      case "formGroup":
        values[property.fieldKey] =
          options.respectExistingValues && values[property.fieldKey] ? values[property.fieldKey] : {};
        setDefaultValues(property, values[property.fieldKey] as Record<string, unknown>, options);
        break;
      case "objectArray":
        if (property.isRequired && !(options.respectExistingValues && values[property.fieldKey])) {
          values[property.fieldKey] = [];
        }
        break;
      case "formCondition":
        values[property.fieldKey] = {};
        let chosenCondition = property.conditions[0];
        // if default is set, try to find it in the list of possible selection const values.
        // if there is a match, default to this condition.
        // In all other cases, go with the first one.
        if (property.default) {
          const matchingConditionIndex = property.selectionConstValues.indexOf(property.default);
          if (matchingConditionIndex !== -1) {
            chosenCondition = property.conditions[matchingConditionIndex];
          }
        }

        setDefaultValues(chosenCondition, values[property.fieldKey] as Record<string, unknown>);
    }
  });
}

export function useBuildForm(
  isEditMode: boolean,
  formType: "source" | "destination",
  selectedConnectorDefinitionSpecification:
    | ConnectorDefinitionSpecification
    | SourceDefinitionSpecificationDraft
    | undefined,
  initialValues?: Partial<ConnectorFormValues>
): BuildFormHook {
  const { formatMessage } = useIntl();
  const allowOAuthConnector = useFeature(FeatureItem.AllowOAuthConnector);

  const isDraft =
    selectedConnectorDefinitionSpecification &&
    isSourceDefinitionSpecificationDraft(selectedConnectorDefinitionSpecification);

  try {
    const jsonSchema: AirbyteJSONSchema = useMemo(() => {
      if (!selectedConnectorDefinitionSpecification) {
        return {
          type: "object",
          properties: {},
        };
      }
      const schema: AirbyteJSONSchema = {
        type: "object",
        properties: {
          connectionConfiguration:
            selectedConnectorDefinitionSpecification.connectionSpecification as JSONSchema7Definition,
        },
      };
      if (isDraft) {
        return schema;
      }
      schema.properties = {
        name: {
          type: "string",
          title: formatMessage({ id: `form.${formType}Name` }),
          description: formatMessage({ id: `form.${formType}Name.message` }),
          // setting order and group to ensure that the name input comes first on the form in a separate group
          order: Number.MIN_SAFE_INTEGER,
          group: NAME_GROUP_ID,
        },
        ...schema.properties,
      };
      schema.required = ["name", "connectionConfiguration"];
      return schema;
    }, [formType, formatMessage, isDraft, selectedConnectorDefinitionSpecification]);

    const formBlock = useMemo<FormBlock>(() => jsonSchemaToFormBlock(jsonSchema), [jsonSchema]);

    if (formBlock._type !== "formGroup") {
      throw new FormBuildError("connectorForm.error.topLevelNonObject");
    }

    const validationSchema = useMemo(
      () => buildYupFormForJsonSchema(jsonSchema, formBlock, formatMessage),
      [formBlock, formatMessage, jsonSchema]
    );

    const startValues = useMemo<ConnectorFormValues>(() => {
      let baseValues = {
        name: "",
        connectionConfiguration: {},
        ...initialValues,
      };

      if (isDraft) {
        try {
          baseValues = validationSchema.cast(baseValues, { stripUnknown: true });
        } catch {
          // cast did not work which can happen if there are unexpected values in the form. Reset form in this case
          baseValues.connectionConfiguration = {};
        }
      }

      if (isEditMode) {
        return baseValues;
      }

      setDefaultValues(formBlock, baseValues as Record<string, unknown>, { respectExistingValues: Boolean(isDraft) });

      return baseValues;
    }, [formBlock, initialValues, isDraft, isEditMode, validationSchema]);

    // flatten out the connectionConfiguration properties so they are displayed properly in the ConnectorForm
    const flattenedFormFields = useMemo(
      () =>
        formBlock.properties.flatMap((block) =>
          block._type === "formGroup" && block.fieldKey === "connectionConfiguration"
            ? allowOAuthConnector && authPredicateMatchesPath(block.fieldKey, selectedConnectorDefinitionSpecification)
              ? // OAuth button needs to be rendered at root level, so keep around an empty FormGroup for it to be rendered in.
                // Order it after name and before the rest of the fields.
                [{ ...block, properties: [], order: Number.MIN_SAFE_INTEGER + 1 }, ...block.properties]
              : block.properties
            : block
        ),
      [allowOAuthConnector, formBlock.properties, selectedConnectorDefinitionSpecification]
    );

    const groups: GroupDetails[] = useMemo(() => {
      // ensure that the name group comes first
      const baseGroups = [{ id: NAME_GROUP_ID }];
      const spec = selectedConnectorDefinitionSpecification?.connectionSpecification;
      if (!spec || typeof spec !== "object" || !("groups" in spec) || !Array.isArray(spec.groups)) {
        return baseGroups;
      }
      return [
        ...baseGroups,
        ...spec.groups.map(({ id, title }) => {
          return {
            id,
            title,
          };
        }),
      ];
    }, [selectedConnectorDefinitionSpecification]);

    return {
      initialValues: startValues,
      formFields: flattenedFormFields,
      validationSchema,
      groups,
    };
  } catch (e) {
    // catch and re-throw form-build errors to enrich them with the connector id
    if (isFormBuildError(e)) {
      throw new FormBuildError(
        e.message,
        isDraft || !selectedConnectorDefinitionSpecification
          ? undefined
          : ConnectorSpecification.id(selectedConnectorDefinitionSpecification)
      );
    }
    throw e;
  }
}
