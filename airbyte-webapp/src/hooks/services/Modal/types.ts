import React from "react";

import { ModalProps } from "components/ui/Modal";

export interface ModalOptions<T> {
  title?: ModalProps["title"];
  size?: ModalProps["size"];
  preventCancel?: boolean;
  content: React.ComponentType<ModalContentProps<T>>;
  testId?: string;
  allowNavigation?: boolean;
}

export type ModalResult<T> = { type: "canceled" } | { type: "completed"; reason: T };

export interface ModalContentProps<T> {
  onComplete: (result: T) => void;
  onCancel: () => void;
}

export interface ModalServiceContext {
  openModal: <ResultType>(options: ModalOptions<ResultType>) => Promise<ModalResult<ResultType>>;
  getCurrentModalTitle: () => ModalProps["title"] | undefined;
}
