import React from 'react';
import { MultiSelect, Option } from '@/components/ui/multi-select';

type Props = {
  project: string;
  value: string[];
  onChange: (value: string[]) => void;
  options: string[];
};

const DateMultiSelect: React.FC<Props> = ({ value, onChange, options }) => {
  const opt: Option[] = options.map((d) => ({ label: d, value: d }));
  return (
    <MultiSelect
      value={value}
      onChange={onChange}
      options={opt}
      placeholder="选择日期"
      className="w-[220px]"
    />
  );
};

export default DateMultiSelect;
