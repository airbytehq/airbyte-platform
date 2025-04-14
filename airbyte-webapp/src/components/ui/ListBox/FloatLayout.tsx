import { Float } from "@headlessui-float/react";
import { FloatProps } from "@headlessui-float/react/dist/float";

export const FloatLayout: React.FC<FloatProps> = ({ children, ...restProps }) => (
  <Float
    placement="bottom-start"
    flip={15}
    offset={5} // $spacing-sm
    shift={5} // $spacing-sm
    autoUpdate={{
      elementResize: false, // this will prevent render in wrong place after multiple open/close actions
    }}
    {...restProps}
  >
    {children}
  </Float>
);
