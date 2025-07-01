import { Placement } from "@floating-ui/react-dom";

import { DropdownMenu, DropdownMenuOptions, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { IconProps } from "components/ui/Icon";

import { NavItem } from "./NavItem";

interface NavDropdownProps {
  className?: string;
  options: DropdownMenuOptions;
  label?: React.ReactNode;
  icon: IconProps["type"];
  onChange?: (data: DropdownMenuOptionType) => false | void;
  buttonTestId?: string;
  placement?: Placement;
}

export const NavDropdown: React.FC<NavDropdownProps> = ({
  className,
  options,
  icon,
  label,
  onChange,
  buttonTestId,
  placement = "right-end",
}) => {
  return (
    <DropdownMenu placement={placement} displacement={10} options={options} onChange={onChange}>
      {({ open }) => (
        <NavItem as="button" className={className} testId={buttonTestId} label={label} icon={icon} isActive={open} />
      )}
    </DropdownMenu>
  );
};
