import React from "react";

export const IsAirbyteEmbeddedContext = React.createContext<boolean | undefined>(undefined);

export const useIsAirbyteEmbeddedContext = () => {
  const context = React.useContext(IsAirbyteEmbeddedContext);
  if (context === undefined) {
    return false;
  }
  return context;
};
