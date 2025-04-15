import React, { PropsWithChildren, createContext, useCallback, useContext, useMemo, useState } from "react";
import { createPortal } from "react-dom";

import { Drawer } from "components/ui/Drawer";

interface DrawerActions {
  openDrawer: ({ title, content }: { title: React.ReactNode; content: React.ReactNode }) => void;
  closeDrawer: () => void;
}
const drawerActions = createContext<DrawerActions | undefined>(undefined);
export const useDrawerActions = () => {
  const context = useContext(drawerActions);
  if (!context) {
    throw new Error("useDrawerActions must be used within a drawerActions context provider");
  }
  return context;
};

// In most cases, consumers only need access to the actions of the drawer. The state is in a separate context so that it
// does not cause re-renders to consumers of useDrawerActions() when it changes.
interface DrawerState {
  isDrawerOpen: boolean;
}
const drawerState = createContext<DrawerState | undefined>(undefined);
export const useDrawerState = () => {
  const context = useContext(drawerState);
  if (!context) {
    throw new Error("useDrawerState must be used within a drawerState context provider");
  }
  return context;
};

export const DrawerContextProvider: React.FC<PropsWithChildren> = ({ children }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [title, setTitle] = useState<React.ReactNode>(null);
  const [body, setBody] = useState<React.ReactNode>(null);

  const openDrawer = useCallback(
    ({ title, content }: { title: React.ReactNode; content: React.ReactNode }) => {
      setTitle(title);
      setBody(content);
      setIsOpen(true);
    },
    [setTitle, setBody, setIsOpen]
  );

  const closeDrawer = useCallback(() => {
    setIsOpen(false);
  }, [setIsOpen]);

  const actionsContextValues = useMemo(
    () => ({
      openDrawer,
      closeDrawer,
    }),
    [openDrawer, closeDrawer]
  );

  const stateContextValues = useMemo(
    () => ({
      isDrawerOpen: isOpen,
      drawerTitle: title,
      drawerBody: body,
    }),
    [isOpen, title, body]
  );

  return (
    <drawerActions.Provider value={actionsContextValues}>
      <drawerState.Provider value={stateContextValues}>
        <>
          {createPortal(
            <Drawer isOpen={isOpen} onClose={closeDrawer} title={title}>
              {body}
            </Drawer>,
            document.body
          )}
          {children}
        </>
      </drawerState.Provider>
    </drawerActions.Provider>
  );
};
