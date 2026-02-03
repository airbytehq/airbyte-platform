import { ConfirmationModalProps } from "components/ui/ConfirmationModal/ConfirmationModal";

export type ConfirmationModalOptions = Omit<ConfirmationModalProps, "onCancel"> & {
  onCancel?: ConfirmationModalProps["onCancel"];
};

export interface ConfirmationModalServiceApi {
  openConfirmationModal: (confirmationModal: ConfirmationModalOptions) => void;
  closeConfirmationModal: () => void;
}

export interface ConfirmationModalState {
  isOpen: boolean;
  confirmationModal: ConfirmationModalOptions | null;
}
