import { createContext, useContext, useState } from "react";

import { defaultBuilderStateSchema } from "components/connectorBuilder/types";
import { AirbyteJsonSchema } from "components/forms/SchemaForm/utils";

interface SchemaContext {
  builderStateSchema: AirbyteJsonSchema;
  setBuilderStateSchema: React.Dispatch<React.SetStateAction<AirbyteJsonSchema>>;
}

export const ConnectorBuilderSchemaContext = createContext<SchemaContext | null>(null);

export const ConnectorBuilderSchemaProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const [schema, setSchema] = useState<AirbyteJsonSchema>(defaultBuilderStateSchema);

  return (
    <ConnectorBuilderSchemaContext.Provider
      value={{
        builderStateSchema: schema,
        setBuilderStateSchema: setSchema,
      }}
    >
      {children}
    </ConnectorBuilderSchemaContext.Provider>
  );
};

export const useConnectorBuilderSchema = (): SchemaContext => {
  const connectorBuilderSchema = useContext(ConnectorBuilderSchemaContext);
  if (!connectorBuilderSchema) {
    throw new Error("useConnectorBuilderResolve must be used within a ConnectorBuilderResolveProvider.");
  }

  return connectorBuilderSchema;
};
