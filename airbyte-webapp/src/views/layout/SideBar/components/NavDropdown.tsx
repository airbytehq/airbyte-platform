import classNames from "classnames";

import { DropdownMenu, DropdownMenuOptionType, DropdownMenuOptions } from "components/ui/DropdownMenu";
import { Text } from "components/ui/Text";

import styles from "./NavDropdown.module.scss";

interface NavDropdownProps {
  options: DropdownMenuOptions;
  label: React.ReactNode;
  icon: React.ReactNode;
  onChange?: (data: DropdownMenuOptionType) => false | void;
}

export const NavDropdown: React.FC<NavDropdownProps> = ({ options, icon, label, onChange }) => {
  return (
    <DropdownMenu placement="right" displacement={10} options={options} onChange={onChange}>
      {({ open }) => (
        <button className={classNames(styles.dropdownMenuButton, styles.menuItem, { [styles.open]: open })}>
          {icon}
          <Text size="sm">{label}</Text>
        </button>
      )}
    </DropdownMenu>
  );
};
