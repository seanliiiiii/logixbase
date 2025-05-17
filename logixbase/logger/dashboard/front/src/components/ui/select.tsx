// src/components/ui/select.tsx
import * as React from 'react';
import { cn } from './utils';

export type Option = {
  value: string;
  label: string;
};

type SelectProps = {
  value: string;
  onChange: (val: string) => void;
  options: Option[];
  className?: string;
};

export const Select: React.FC<SelectProps> = ({
  value,
  onChange,
  options,
  className,
}) => {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className={cn(
        'block w-full rounded border border-gray-300 bg-white px-2 py-1 text-sm',
        'focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500',
        className
      )}
    >
      {options.map((opt) => (
        <option key={opt.value} value={opt.value}>
          {opt.label}
        </option>
      ))}
    </select>
  );
};
