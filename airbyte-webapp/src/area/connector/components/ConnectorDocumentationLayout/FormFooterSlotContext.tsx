import { createContext, useContext } from "react";

/**
 * The element at the bottom of the form panel that holds the connector form's footer buttons.
 * It is null outside `ConnectorDocumentationLayout`, and the footer then renders in place.
 */
export const FormFooterSlotContext = createContext<HTMLElement | null>(null);

export const useFormFooterSlot = () => useContext(FormFooterSlotContext);
