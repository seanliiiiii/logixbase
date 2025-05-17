// src/components/ui/datepicker.tsx
import React from 'react';
import { cn } from './utils';

type DatePickerProps = {
  value?: string;                // 当前日期值，如 "2025-04-10"
  onChange: (value: string) => void;
  className?: string;
};

export const DatePicker: React.FC<DatePickerProps> = ({
  value,
  onChange,
  className,
}) => {
  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange(e.target.value);
  };

  return (
    <input
      type="date"
      value={value}
      onChange={handleChange}
      className={cn(
        'rounded border border-gray-300 bg-white px-2 py-1 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500',
        className
      )}
    />
  );
};
