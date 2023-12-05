import classNames from "classnames";
import { Multiselect as ReactMultiselect } from "react-widgets";
import { MultiselectProps as WidgetMultiselectProps } from "react-widgets/lib/Multiselect";

import styles from "./Multiselect.module.scss";
import "react-widgets/dist/css/react-widgets.css";

export interface MultiselectProps extends WidgetMultiselectProps {
  disabled?: boolean;
  error?: boolean;
  name?: string;
}

export const Multiselect: React.FC<MultiselectProps> = (props) => {
  return (
    <div className={classNames(styles.container, { [styles.error]: props.error })}>
      <ReactMultiselect {...props} />
    </div>
  );
};
