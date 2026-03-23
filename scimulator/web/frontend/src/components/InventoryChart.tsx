import { useEffect, useState, useRef } from 'react'
import { getInventoryTimeseries } from '../api/client'
import type { InventoryTimeseries } from '../api/client'

// Chart.js loaded via CDN
// eslint-disable-next-line @typescript-eslint/no-explicit-any
declare global {
  interface Window {
    Chart: any
  }
}

const COLORS = [
  '#4e79a7', '#f28e2b', '#e15759', '#76b7b2',
  '#59a14f', '#edc948', '#b07aa1', '#ff9da7',
  '#9c755f', '#bab0ac',
]

export default function InventoryChart({ dbName, scenarioId }: { dbName: string; scenarioId: string }) {
  const [data, setData] = useState<InventoryTimeseries | null>(null)
  const [groupBy, setGroupBy] = useState<'node' | 'product' | 'total'>('node')
  const [loading, setLoading] = useState(true)
  const [chartReady, setChartReady] = useState(false)
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const chartRef = useRef<unknown>(null)

  // Load Chart.js from CDN
  useEffect(() => {
    if (window.Chart) {
      setChartReady(true)
      return
    }
    const script = document.createElement('script')
    script.src = 'https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js'
    script.onload = () => setChartReady(true)
    document.head.appendChild(script)
  }, [])

  // Fetch data
  useEffect(() => {
    setLoading(true)
    getInventoryTimeseries(dbName, scenarioId, groupBy)
      .then(d => {
        setData(d)
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [dbName, scenarioId, groupBy])

  // Render chart
  useEffect(() => {
    if (!data || !canvasRef.current || !chartReady || !window.Chart) return

    // Destroy previous chart
    if (chartRef.current) {
      (chartRef.current as { destroy: () => void }).destroy()
    }

    const seriesNames = Object.keys(data.series)
    const datasets = seriesNames.map((name, i) => ({
      label: name,
      data: data.series[name],
      borderColor: COLORS[i % COLORS.length],
      backgroundColor: COLORS[i % COLORS.length] + '20',
      fill: seriesNames.length === 1,
      tension: 0.1,
      pointRadius: 0,
    }))

    chartRef.current = new window.Chart(canvasRef.current, {
      type: 'line',
      data: {
        labels: data.dates,
        datasets,
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
          mode: 'index',
          intersect: false,
        },
        scales: {
          x: {
            title: { display: true, text: 'Date' },
            ticks: { maxTicksLimit: 15 },
          },
          y: {
            title: { display: true, text: 'Saleable Inventory (units)' },
            beginAtZero: true,
          },
        },
        plugins: {
          legend: { position: 'top' },
        },
      },
    })

    return () => {
      if (chartRef.current) {
        (chartRef.current as { destroy: () => void }).destroy()
      }
    }
  }, [data, chartReady])

  return (
    <div>
      <div className="chart-controls">
        <label>Group by: </label>
        <select value={groupBy} onChange={e => setGroupBy(e.target.value as 'node' | 'product' | 'total')}>
          <option value="node">Node</option>
          <option value="product">Product</option>
          <option value="total">Total</option>
        </select>
      </div>

      {loading ? (
        <p>Loading chart data...</p>
      ) : (
        <div className="chart-container">
          <canvas ref={canvasRef} />
        </div>
      )}
    </div>
  )
}
