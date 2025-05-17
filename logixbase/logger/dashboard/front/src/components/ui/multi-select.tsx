// src/components/ui/multi-select.tsx
import * as React from 'react';
import { cn } from './utils';

export type Option = {
  label: string;
  value: string;
};

type MultiSelectProps = {
  value: string[];                 // 已选值
  options: Option[];               // 可选项
  onChange: (val: string[]) => void;
  placeholder?: string;
  className?: string;
};

export const MultiSelect: React.FC<MultiSelectProps> = ({
  value = [],
  options = [],
  onChange,
  placeholder = '请选择',
  className,
}) => {
  const toggle = (val: string) => {
    if (value.includes(val)) {
      onChange(value.filter((v) => v !== val));
    } else {
      onChange([...value, val]);
    }
  };

  return (
    <div className={cn('border rounded px-2 py-1 bg-white text-sm w-[220px]', className)}>
      <div className="text-gray-500 mb-1">{placeholder}</div>
      <div className="flex flex-col gap-1 max-h-40 overflow-y-auto pr-1">
        {options.length > 0 ? (
          options.map((opt) => (
            <label key={opt.value} className="flex items-center gap-1 cursor-pointer">
              <input
                type="checkbox"
                checked={value.includes(opt.value)}
                onChange={() => toggle(opt.value)}
                className="accent-blue-600"
              />
              <span>{opt.label}</span>
            </label>
          ))
        ) : (
          <div className="text-gray-400 italic">暂无可选项</div>
        )}
      </div>
    </div>
  );
};
