// src/pages/DashboardPage.tsx
import React, { useEffect, useState } from 'react';
import ProjectSelector from '@/components/ProjectSelector';
import LevelCheckboxGroup from '@/components/LevelCheckboxGroup';
import LogTable from '@/components/LogTable';
import { DatePicker } from '@/components/ui/datepicker';

const DEFAULT_LEVELS = ['INFO', 'WARNING', 'ERROR', 'DEBUG'];

const DashboardPage: React.FC = () => {
  const [projects, setProjects] = useState<string[]>([]);
  const [selectedProject, setSelectedProject] = useState('');
  const [selectedLevels, setSelectedLevels] = useState<string[]>(DEFAULT_LEVELS);
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [logs, setLogs] = useState<any[]>([]);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [total, setTotal] = useState(0);
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [flash, setFlash] = useState(false);
  const [loading, setLoading] = useState(false);
  const [refreshCount, setRefreshCount] = useState(0); // ✅ 用于强制刷新

  // 获取项目列表
  useEffect(() => {
    fetch('/api/projects')
      .then(res => res.json())
      .then(data => {
        if (Array.isArray(data)) {
          setProjects(data);
          if (data.length > 0) setSelectedProject(data[0]);
        }
      });
  }, []);

  // 加载日志数据（支持刷新触发）
  useEffect(() => {
    if (!selectedProject || !startDate || !endDate) return;

    setLoading(true);
    fetch('/api/logs/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        project: selectedProject,
        dates: [],
        level: selectedLevels,
        page,
        page_size: pageSize,
        sort_by: 'timestamp',
        order: sortOrder,
        start_time: startDate,
        end_time: endDate,
      }),
    })
      .then(res => res.json())
      .then(data => {
        setLogs(data.logs || []);
        setTotal(data.total || 0);
      })
      .finally(() => {
        setLoading(false);
      });
  }, [selectedProject, selectedLevels, startDate, endDate, page, pageSize, sortOrder, refreshCount]); // ✅ 刷新触发

  // 刷新按钮逻辑
  const handleRefresh = () => {
    setFlash(true);
    setTimeout(() => setFlash(false), 200);
    setPage(1);
    setSortOrder('desc');
    setRefreshCount(c => c + 1); // ✅ 显式触发 useEffect
  };

  return (
    <div className="p-4 space-y-4">
      {/* 条件筛选区域 */}
      <div className="flex flex-wrap items-center gap-4">
        <div className="text-sm font-medium">指定项目</div>
        <ProjectSelector
          projects={projects}
          selectedProject={selectedProject}
          onChange={project => {
            setSelectedProject(project);
            setPage(1);
          }}
        />

        <div className="flex items-center gap-2">
          <span className="text-sm">开始日期:</span>
          <DatePicker value={startDate} onChange={setStartDate} />
        </div>
        <div className="flex items-center gap-2">
          <span className="text-sm">结束日期:</span>
          <DatePicker value={endDate} onChange={setEndDate} />
        </div>

        <LevelCheckboxGroup value={selectedLevels} onChange={setSelectedLevels} />

        <button
          onClick={handleRefresh}
          className="px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 active:scale-95 transition-transform"
        >
          🔄 刷新
        </button>
      </div>

      {/* 加载中状态提示 */}
      {loading && (
        <div className="flex items-center gap-2 text-sm text-gray-600">
          <div className="w-4 h-4 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
          正在加载数据...
        </div>
      )}

      {/* 表格区域 */}
      <div className={flash ? 'animate-pulse' : ''}>
        <LogTable
          logs={logs}
          total={total}
          page={page}
          pageSize={pageSize}
          onPageChange={setPage}
          onPageSizeChange={size => {
            setPageSize(size);
            setPage(1);
          }}
          sortOrder={sortOrder}
          onSortToggle={() => setSortOrder(o => (o === 'asc' ? 'desc' : 'asc'))}
        />
      </div>
    </div>
  );
};

export default DashboardPage;
