import { yupResolver } from "@hookform/resolvers/yup";
import { zodResolver } from "@hookform/resolvers/zod";
import classNames from "classnames";
import { ReactNode, useEffect } from "react";
import {
  useForm,
  FormProvider,
  KeepStateOptions,
  DefaultValues,
  UseFormReturn,
  UseFormProps,
  FieldValues,
} from "react-hook-form";
import * as yup from "yup";
import { z } from "zod";

import styles from "./Form.module.scss";
import { FormChangeTracker } from "./FormChangeTracker";
import { FormDevTools } from "./FormDevTools";

export type FormValues = FieldValues;

export type FormSubmissionHandler<T extends FormValues> = (
  values: T,
  methods: UseFormReturn<T>
) => Promise<void | { keepStateOptions?: KeepStateOptions; resetValues?: T }>;

interface FormProps<T extends FormValues> {
  /**
   * The function that will be called when the form is submitted. This function should return a promise that only resolves after the submission has been handled by the upstream service.
   * The return value of this function will be used to determine which parts of the form should be reset.
   */
  onSubmit?: FormSubmissionHandler<T>;
  onSuccess?: (values: T) => void;
  onError?: (e: Error, values: T, methods: UseFormReturn<T>) => void;
  /**
   * Yup schema for form validation
   */
  schema?: yup.SchemaOf<T>;
  /**
   * Zod schema for form validation
   */
  zodSchema?: z.ZodSchema<T>;
  defaultValues: DefaultValues<T>;
  children?: ReactNode | undefined;
  trackDirtyChanges?: boolean;
  mode?: UseFormProps<T>["mode"];
  reValidateMode?: UseFormProps<T>["reValidateMode"];
  /**
   * Reinitialize form values when defaultValues changes. This will only work if the form is not dirty. Defaults to false.
   */
  reinitializeDefaultValues?: boolean;
  /**
   * Disable all form controls including submission buttons. Defaults to false.
   */
  disabled?: boolean;
  dataTestId?: string;
  /**
   * A unique identifier for tracking changes to the form, integrates with the form change tracking service
   */
  formTrackerId?: string;
}

export const Form = <T extends FormValues>({
  children,
  onSubmit,
  onSuccess,
  onError,
  defaultValues,
  schema,
  zodSchema,
  trackDirtyChanges = false,
  reinitializeDefaultValues = false,
  mode = "onChange",
  reValidateMode,
  disabled = false,
  dataTestId,
  formTrackerId,
}: FormProps<T>) => {
  const methods = useForm<T>({
    defaultValues,
    resolver: zodSchema ? zodResolver(zodSchema) : yupResolver((schema ?? yup.object()) as yup.ObjectSchema<T>),
    mode,
    reValidateMode,
  });

  useEffect(() => {
    if (reinitializeDefaultValues && !methods.formState.isDirty) {
      methods.reset(defaultValues);
    }
  }, [reinitializeDefaultValues, defaultValues, methods]);

  const processSubmission = (values: T) => {
    if (!onSubmit) {
      return;
    }

    return onSubmit(values, methods)
      .then((submissionResult) => {
        onSuccess?.(values);
        if (submissionResult) {
          methods.reset(submissionResult.resetValues ?? values, submissionResult.keepStateOptions ?? undefined);
        } else {
          methods.reset(values);
        }
      })
      .catch((e) => {
        onError?.(e, values, methods);
      });
  };

  return (
    <FormProvider {...methods}>
      <FormDevTools />
      {trackDirtyChanges && <FormChangeTracker formId={formTrackerId} changed={methods.formState.isDirty} />}
      <form onSubmit={methods.handleSubmit(processSubmission)} data-testid={dataTestId} className={styles.flex}>
        <fieldset disabled={disabled} className={classNames(styles.fieldset, styles.flex)}>
          {children}
        </fieldset>
      </form>
    </FormProvider>
  );
};
