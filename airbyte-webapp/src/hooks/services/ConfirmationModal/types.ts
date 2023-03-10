import { ConfirmationModalProps } from "components/common/ConfirmationModal/ConfirmationModal";

export type ConfirmationModalOptions = Omit<ConfirmationModalProps, "onClose"> & {
  onClose?: ConfirmationModalProps["onClose"];
};

export interface ConfirmationModalServiceApi {
  openConfirmationModal: (confirmationModal: ConfirmationModalOptions) => void;
  closeConfirmationModal: () => void;
}

export interface ConfirmationModalState {
  isOpen: boolean;
  confirmationModal: ConfirmationModalOptions | null;
}
