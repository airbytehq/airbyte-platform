import { yupResolver } from "@hookform/resolvers/yup";
import { ReactNode } from "react";
import { useForm, FormProvider, DeepPartial } from "react-hook-form";
import { SchemaOf } from "yup";

import { FormChangeTracker } from "components/common/FormChangeTracker";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type FormValues = Record<string, any>;

interface FormProps<T extends FormValues> {
  onError?: (e: Error, values: T) => void;
  onSubmit: (values: T) => Promise<unknown>;
  onSuccess?: (values: T) => void;
  schema: SchemaOf<T>;
  defaultValues: DeepPartial<T>;
  children?: ReactNode | undefined;
  trackDirtyChanges?: boolean;
}

export const Form = <T extends FormValues>({
  children,
  onSubmit,
  onSuccess,
  onError,
  defaultValues,
  schema,
  trackDirtyChanges = false,
}: FormProps<T>) => {
  const methods = useForm<T>({
    defaultValues,
    resolver: yupResolver(schema),
    mode: "onChange",
  });

  const processSubmission = (values: T) =>
    onSubmit(values)
      .then(() => {
        onSuccess?.(values);
        methods.reset(values);
      })
      .catch((e) => {
        onError?.(e, values);
      });

  return (
    <FormProvider {...methods}>
      {trackDirtyChanges && <FormChangeTracker changed={methods.formState.isDirty} />}
      <form onSubmit={methods.handleSubmit((values) => processSubmission(values))}>{children}</form>
    </FormProvider>
  );
};
