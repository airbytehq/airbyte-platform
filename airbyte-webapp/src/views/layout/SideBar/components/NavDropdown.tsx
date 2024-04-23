import { DropdownMenu, DropdownMenuOptions, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { IconProps } from "components/ui/Icon";

import { NavItem } from "./NavItem";

interface NavDropdownProps {
  options: DropdownMenuOptions;
  label: React.ReactNode;
  icon: IconProps["type"];
  onChange?: (data: DropdownMenuOptionType) => false | void;
  buttonTestId?: string;
}

export const NavDropdown: React.FC<NavDropdownProps> = ({ options, icon, label, onChange, buttonTestId }) => {
  return (
    <DropdownMenu placement="right-end" displacement={10} options={options} onChange={onChange}>
      {({ open }) => <NavItem as="button" testId={buttonTestId} label={label} icon={icon} isActive={open} />}
    </DropdownMenu>
  );
};
