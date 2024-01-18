import { DropdownMenu, DropdownMenuOptionType, DropdownMenuOptions } from "components/ui/DropdownMenu";

import { NavItem } from "./NavItem";

interface NavDropdownProps {
  options: DropdownMenuOptions;
  label: React.ReactNode;
  icon: React.ReactNode;
  onChange?: (data: DropdownMenuOptionType) => false | void;
}

export const NavDropdown: React.FC<NavDropdownProps> = ({ options, icon, label, onChange }) => {
  return (
    <DropdownMenu placement="right-end" displacement={10} options={options} onChange={onChange}>
      {({ open }) => <NavItem as="button" label={label} icon={icon} isActive={open} />}
    </DropdownMenu>
  );
};
