// src/components/ui/card.tsx
import React from 'react';

type Props = {
  children: React.ReactNode;
  className?: string;
};

export const Card: React.FC<Props> = ({ children, className = '' }) => {
  return (
    <div className={`border border-gray-200 rounded-lg p-4 shadow-sm ${className}`}>
      {children}
    </div>
  );
};

