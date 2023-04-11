import { yupResolver } from "@hookform/resolvers/yup";
import { ReactNode } from "react";
import { useForm, FormProvider, DeepPartial } from "react-hook-form";
import { ObjectSchema } from "yup";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type FormValues = Record<string, any>;

interface FormProps<T extends FormValues> {
  onSubmit: (values: T) => Promise<unknown>;
  schema: ObjectSchema<T>;
  defaultValues: DeepPartial<T>;
  children?: ReactNode | undefined;
}

export const Form = <T extends FormValues>({ children, onSubmit, defaultValues, schema }: FormProps<T>) => {
  const methods = useForm<T>({
    defaultValues,
    resolver: yupResolver(schema),
    mode: "onChange",
  });

  return (
    <FormProvider {...methods}>
      <form onSubmit={methods.handleSubmit(onSubmit)}>{children}</form>
    </FormProvider>
  );
};
