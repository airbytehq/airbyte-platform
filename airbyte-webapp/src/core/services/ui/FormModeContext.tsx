import React, { createContext, useContext, ReactNode } from "react";

export type FormMode = "create" | "edit" | "readonly";

interface FormModeContextValue {
  mode: FormMode;
}

const FormModeContext = createContext<FormModeContextValue | undefined>(undefined);

export const FormModeProvider: React.FC<{ children: ReactNode; mode: FormMode }> = ({ children, mode }) => {
  return <FormModeContext.Provider value={{ mode }}>{children}</FormModeContext.Provider>;
};

export const useFormMode = (): FormModeContextValue => {
  const context = useContext(FormModeContext);
  if (!context) {
    throw new Error("useFormMode must be used within a FormModeProvider");
  }
  return context;
};
