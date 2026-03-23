import React from 'react';

export function DateDisplay({ currentTime }) {
  const d = new Date(currentTime);
  const formatted = d.toLocaleDateString('en-US', {
    weekday: 'short',
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  }) + ' ' + d.toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });

  return (
    <div className="date-display">
      {formatted}
    </div>
  );
}
