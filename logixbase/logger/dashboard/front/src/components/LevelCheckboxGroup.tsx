// src/components/LevelCheckboxGroup.tsx
import React, { useMemo } from 'react';
import { Checkbox } from '@/components/ui-kit';

const LEVELS = ['INFO', 'WARNING', 'ERROR', 'DEBUG'];

type Props = {
  value: string[];
  onChange: (value: string[]) => void;
};

const LevelCheckboxGroup: React.FC<Props> = ({ value = [], onChange }) => {
  const allSelected = useMemo(() => value.length === LEVELS.length, [value]);

  const toggle = (level: string) => {
    if (value.includes(level)) {
      onChange(value.filter((v) => v !== level));
    } else {
      onChange([...value, level]);
    }
  };

  const toggleAll = () => {
    if (allSelected) {
      onChange([]);
    } else {
      onChange([...LEVELS]);
    }
  };

  return (
    <div className="flex items-center gap-4">
      <div className="flex items-center gap-3">
        {LEVELS.map((level) => (
          <label key={level} className="flex items-center gap-1 text-sm">
            <Checkbox
              checked={value.includes(level)}
              onCheckedChange={() => toggle(level)}
            />
            {level}
          </label>
        ))}
      </div>
      <button
        onClick={toggleAll}
        className="px-2 py-1 border rounded text-sm hover:bg-gray-100"
      >
        {allSelected ? '取消全选' : '全选'}
      </button>
    </div>
  );
};

export default LevelCheckboxGroup;
