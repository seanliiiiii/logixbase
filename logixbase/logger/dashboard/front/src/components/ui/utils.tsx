// src/components/ui/utils.ts
/**
 * 常用工具函数，例如合并类名。
 */
export function cn(...classes: (string | undefined | null | false)[]) {
  return classes.filter(Boolean).join(' ');
}
