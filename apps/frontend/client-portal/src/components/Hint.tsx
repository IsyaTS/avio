import React from 'react';

const Hint: React.FC<{ text: string }> = ({ text }) => (
  <span
    className="group relative inline-flex h-4 w-4 items-center justify-center rounded-full bg-slate-200 text-[10px] font-bold text-slate-600 cursor-help"
    aria-label={text}
    title={text}
  >
    !
    <span className="pointer-events-none absolute bottom-full left-1/2 z-30 hidden w-64 -translate-x-1/2 rounded-lg bg-slate-900 px-2 py-1.5 text-xs text-white shadow-lg group-hover:block">
      {text}
    </span>
  </span>
);

export default Hint;
