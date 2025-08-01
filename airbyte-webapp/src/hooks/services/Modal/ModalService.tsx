import React, { useCallback, useContext, useMemo, useRef, useState } from "react";
import { firstValueFrom, Subject } from "rxjs";

import { LoadingPage } from "components";
import { Modal } from "components/ui/Modal";

import { ModalOptions, ModalResult, ModalServiceContext } from "./types";

const modalServiceContext = React.createContext<ModalServiceContext | undefined>(undefined);

export const ModalServiceProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  // The any here is due to the fact, that every call to open a modal might come in with
  // a different type, thus we can't type this with unknown or a generic.
  // The consuming code of this service though is properly typed, so that this `any` stays
  // encapsulated within this component.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [modalOptions, setModalOptions] = useState<ModalOptions<any>>();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const resultSubjectRef = useRef<Subject<ModalResult<any>>>();

  // Store the current modal title so it persists even after modal closes
  const currentModalTitleRef = useRef<React.ReactNode | undefined>();

  const getCurrentModalTitle = useCallback(() => currentModalTitleRef.current, []);

  const service: ModalServiceContext = useMemo(
    () => ({
      openModal: async (options) => {
        resultSubjectRef.current = new Subject();
        setModalOptions(options);

        // Capture the modal title when opening
        currentModalTitleRef.current = options.title;

        const reason = await firstValueFrom(resultSubjectRef.current);
        setModalOptions(undefined);
        resultSubjectRef.current = undefined;
        return reason;
      },
      getCurrentModalTitle,
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );

  return (
    <modalServiceContext.Provider value={service}>
      {children}
      {modalOptions && (
        <React.Suspense fallback={<LoadingPage />}>
          <Modal
            title={modalOptions.title}
            size={modalOptions.size}
            testId={modalOptions.testId}
            onCancel={
              modalOptions.preventCancel ? undefined : () => resultSubjectRef.current?.next({ type: "canceled" })
            }
            allowNavigation={modalOptions.allowNavigation}
          >
            <modalOptions.content
              onCancel={() => resultSubjectRef.current?.next({ type: "canceled" })}
              onComplete={(result) => resultSubjectRef.current?.next({ type: "completed", reason: result })}
            />
          </Modal>
        </React.Suspense>
      )}
    </modalServiceContext.Provider>
  );
};

export const useModalService = () => {
  const context = useContext(modalServiceContext);
  if (!context) {
    throw new Error("Can't use ModalService outside ModalServiceProvider");
  }
  return context;
};
