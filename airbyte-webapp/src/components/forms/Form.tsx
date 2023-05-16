import { yupResolver } from "@hookform/resolvers/yup";
import { ReactNode, useEffect } from "react";
import { useForm, FormProvider, DeepPartial, useFormState, KeepStateOptions } from "react-hook-form";
import { SchemaOf } from "yup";

import { FormChangeTracker } from "components/common/FormChangeTracker";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type FormValues = Record<string, any>;

interface FormProps<T extends FormValues> {
  onError?: (e: Error, values: T) => void;
  /**
   * The function that will be called when the form is submitted. This function should return a promise that only resolves after the submission has been handled by the upstream service.
   * The return value of this function will be used to determine which parts of the form should be reset.
   */
  onSubmit: (values: T) => Promise<void | KeepStateOptions>;
  onSuccess?: (values: T) => void;
  schema: SchemaOf<T>;
  defaultValues: DeepPartial<T>;
  children?: ReactNode | undefined;
  trackDirtyChanges?: boolean;
  /**
   * Reinitialize form values when defaultValues changes. This will only work if the form is not dirty. Defaults to false.
   */
  reinitializeDefaultValues?: boolean;
}

const HookFormDirtyTracker = () => {
  const { isDirty } = useFormState();
  return <FormChangeTracker changed={isDirty} />;
};

export const Form = <T extends FormValues>({
  children,
  onSubmit,
  onSuccess,
  onError,
  defaultValues,
  schema,
  trackDirtyChanges = false,
  reinitializeDefaultValues = false,
}: FormProps<T>) => {
  const methods = useForm<T>({
    defaultValues,
    resolver: yupResolver(schema),
    mode: "onChange",
  });

  useEffect(() => {
    if (reinitializeDefaultValues && !methods.formState.isDirty) {
      methods.reset(defaultValues);
    }
  }, [reinitializeDefaultValues, defaultValues, methods]);

  const processSubmission = (values: T) =>
    onSubmit(values)
      .then((keepStateOptions) => {
        onSuccess?.(values);
        methods.reset(values, keepStateOptions ?? undefined);
      })
      .catch((e) => {
        onError?.(e, values);
      });

  return (
    <FormProvider {...methods}>
      {trackDirtyChanges && <HookFormDirtyTracker />}
      <form onSubmit={methods.handleSubmit((values) => processSubmission(values))}>{children}</form>
    </FormProvider>
  );
};
