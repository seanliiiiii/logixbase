import React, { useMemo } from 'react';

type Log = {
  timestamp: string;
  level: string;
  log_id: string;
  thread: string;
  process: string;
  message: string;
};

type Props = {
  logs: Log[];
  total: number;
  page: number;
  pageSize: number;
  sortOrder: 'asc' | 'desc';
  onPageChange: (page: number) => void;
  onPageSizeChange: (size: number) => void;
  onSortToggle: () => void;
};

const PAGE_SIZE_OPTIONS = [20, 50, 100];

const LogTable: React.FC<Props> = ({
  logs = [],
  total,
  page,
  pageSize,
  sortOrder,
  onPageChange,
  onPageSizeChange,
  onSortToggle,
}) => {
  const totalPages = useMemo(() => Math.ceil(total / pageSize), [total, pageSize]);

  return (
    <div className="space-y-4">
      {/* 表格区域 */}
      <div className="w-full overflow-x-auto border rounded">
        <table className="w-full text-sm text-left border-collapse">
          <thead className="bg-gray-100">
            <tr>
              <th
                className="px-3 py-2 border cursor-pointer"
                onClick={onSortToggle}
              >
                时间 <span className="ml-1">{sortOrder === 'asc' ? '🔼' : '🔽'}</span>
              </th>
              <th className="px-3 py-2 border">等级</th>
              <th className="px-3 py-2 border">ID</th>
              <th className="px-3 py-2 border">线程</th>
              <th className="px-3 py-2 border">进程</th>
              <th className="px-3 py-2 border">内容</th>
            </tr>
          </thead>
          <tbody>
            {logs.length === 0 ? (
              <tr>
                <td colSpan={6} className="text-center text-gray-500 py-4">
                  暂无数据
                </td>
              </tr>
            ) : (
              logs.map((log, i) => (
                <tr key={i} className="border-t hover:bg-gray-50">
                  <td className="px-3 py-1 border">{log.timestamp}</td>
                  <td className="px-3 py-1 border">{log.level}</td>
                  <td className="px-3 py-1 border">{log.log_id}</td>
                  <td className="px-3 py-1 border">{log.thread}</td>
                  <td className="px-3 py-1 border">{log.process}</td>
                  <td className="px-3 py-1 border whitespace-pre-wrap">{log.message}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* 分页控制区 */}
      <div className="flex justify-between items-center text-sm">
        <div className="flex items-center gap-2">
          每页：
          {PAGE_SIZE_OPTIONS.map((size) => (
            <button
              key={size}
              onClick={() => onPageSizeChange(size)}
              className={`px-2 py-1 border rounded ${
                pageSize === size ? 'bg-blue-600 text-white' : ''
              }`}
            >
              {size}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => onPageChange(Math.max(page - 1, 1))}
            disabled={page === 1}
            className="px-2 py-1 border rounded disabled:opacity-50"
          >
            上一页
          </button>
          <span>
            第 {page} 页 / 共 {totalPages || 1} 页
          </span>
          <button
            onClick={() => onPageChange(Math.min(page + 1, totalPages))}
            disabled={page >= totalPages}
            className="px-2 py-1 border rounded disabled:opacity-50"
          >
            下一页
          </button>
        </div>
      </div>
    </div>
  );
};

export default LogTable;
