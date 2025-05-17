// src/components/ui/checkbox.tsx
import * as React from 'react';
import { cn } from './utils';

type CheckboxProps = {
  checked: boolean;
  onCheckedChange?: (checked: boolean) => void;
  disabled?: boolean;
  className?: string;
  id?: string;
};

export const Checkbox: React.FC<CheckboxProps> = ({
  checked,
  onCheckedChange,
  disabled,
  className,
  id,
}) => {
  const handleChange = () => {
    if (!disabled && onCheckedChange) {
      onCheckedChange(!checked);
    }
  };

  return (
    <input
      type="checkbox"
      id={id}
      checked={checked}
      onChange={handleChange}
      disabled={disabled}
      className={cn(
        'h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500',
        disabled && 'opacity-50 cursor-not-allowed',
        className
      )}
    />
  );
};
