// src/components/ui/input.tsx
import * as React from 'react';
import { cn } from './utils';

type InputProps = React.InputHTMLAttributes<HTMLInputElement> & {
  className?: string;
};

export const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className, ...props }, ref) => {
    return (
      <input
        ref={ref}
        className={cn(
          'block w-full rounded border border-gray-300 bg-white px-3 py-1.5 text-sm placeholder-gray-400',
          'focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500',
          className
        )}
        {...props}
      />
    );
  }
);

Input.displayName = 'Input';
