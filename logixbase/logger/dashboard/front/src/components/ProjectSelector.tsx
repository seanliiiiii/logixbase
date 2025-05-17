import React from 'react';

type Props = {
  projects: string[];
  selectedProject: string;
  onChange: (value: string) => void;
};

const ProjectSelector: React.FC<Props> = ({ projects = [], selectedProject, onChange }) => {
  return (
    <div className="min-w-[200px]">
      <select
        value={selectedProject}
        onChange={(e) => onChange(e.target.value)}
        className="border rounded px-2 py-1 w-full text-sm"
      >
        {projects.map((p) => (
          <option key={p} value={p}>
            {p}
          </option>
        ))}
      </select>
    </div>
  );
};

export default ProjectSelector;
