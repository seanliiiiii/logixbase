// src/components/ui/button.tsx
import React from 'react';

type Props = {
  onClick: () => void;
  children: React.ReactNode;
  className?: string;
  disabled?: boolean;
};

export const Button: React.FC<Props> = ({
  onClick,
  children,
  className = '',
  disabled = false,
}) => {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`px-4 py-2 border rounded-md text-sm font-medium focus:outline-none focus:ring-2 focus:ring-offset-2
        ${disabled ? 'bg-gray-300 text-gray-500 cursor-not-allowed' : 'bg-blue-600 text-white hover:bg-blue-700 focus:ring-blue-500'}
        ${className}`}
    >
      {children}
    </button>
  );
};
