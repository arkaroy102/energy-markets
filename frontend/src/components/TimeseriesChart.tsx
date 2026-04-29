import { memo, useEffect, useRef, useState } from 'react'
import {
    LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'
import { fetchLocations, fetchTimeseries } from '../api/client'
import type { NodeLocation, PricePoint } from '../types/market'

const GRIDS = ['ERCOT', 'NYISO', 'CAISO']
const LINE_COLORS = ['#4c72b0', '#c44e52', '#55a868', '#dd8452']

function todayLocal(): string {
    return new Date().toLocaleDateString('en-CA')
}

// Convert a UTC HH:MM key back to local time string for display.
// Reconstructs a full Date using the shared date so DST is handled correctly.
function utcKeyToLocalTime(key: string, date: string): string {
    const d = new Date(`${date}T${key}:00Z`)
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

interface SeriesConfig {
    id: number
    grid: string
    node: string
}

interface SeriesData {
    data: PricePoint[]
    loading: boolean
    error: string | null
}

let nextId = 1

function SeriesSelector({
    config,
    locations,
    locationsLoading,
    onChange,
    onRemove,
    color,
    label,
}: {
    config: SeriesConfig
    locations: NodeLocation[]
    locationsLoading: boolean
    onChange: (patch: Partial<SeriesConfig>) => void
    onRemove?: () => void
    color: string
    label: string
}) {
    return (
        <div style={{ display: 'flex', gap: '12px', alignItems: 'center', flexWrap: 'wrap' }}>
            <span style={{ color, fontWeight: 600, minWidth: '60px' }}>{label}</span>

            <label>
                Grid{' '}
                <select value={config.grid} onChange={e => onChange({ grid: e.target.value, node: '' })}>
                    {GRIDS.map(g => <option key={g} value={g}>{g}</option>)}
                </select>
            </label>

            <label>
                Node{' '}
                <select
                    value={config.node}
                    onChange={e => onChange({ node: e.target.value })}
                    disabled={locationsLoading || locations.length === 0}
                >
                    <option value=''>-- select node --</option>
                    {locations.map(loc => (
                        <option key={loc.node_id} value={loc.node_name}>{loc.node_name}</option>
                    ))}
                </select>
            </label>

            {onRemove && (
                <button onClick={onRemove} style={{ cursor: 'pointer' }}>✕ Remove</button>
            )}
        </div>
    )
}

function utcHHMM(timestamp: string): string {
    const d = new Date(timestamp)
    return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`
}

function buildChartData(allSeries: { config: SeriesConfig; data: PricePoint[] }[]): Record<string, unknown>[] {
    const lookup = new Map<number, Map<string, number>>()
    const allKeys = new Set<string>()

    for (const { config, data } of allSeries) {
        const m = new Map<string, number>()
        for (const point of data) {
            const key = utcHHMM(point.timestamp_utc)
            allKeys.add(key)
            m.set(key, point.lmp)
        }
        lookup.set(config.id, m)
    }

    return Array.from(allKeys).sort().map(key => {
        const row: Record<string, unknown> = { time_utc: key }
        for (const { config } of allSeries) {
            row[`lmp_${config.id}`] = lookup.get(config.id)?.get(key) ?? null
        }
        return row
    })
}

export default memo(function TimeseriesChart() {
    const [sharedDate, setSharedDate] = useState(todayLocal())
    const [configs, setConfigs] = useState<SeriesConfig[]>([
        { id: nextId++, grid: 'ERCOT', node: '' },
    ])
    const [seriesData, setSeriesData] = useState<Map<number, SeriesData>>(new Map())
    const [locationsByGrid, setLocationsByGrid] = useState<Map<string, NodeLocation[]>>(new Map())
    const [locationsLoadingGrids, setLocationsLoadingGrids] = useState<Set<string>>(new Set())

    useEffect(() => {
        const gridsNeeded = new Set(configs.map(c => c.grid))
        for (const grid of gridsNeeded) {
            if (!locationsByGrid.has(grid) && !locationsLoadingGrids.has(grid)) {
                setLocationsLoadingGrids(prev => new Set([...prev, grid]))
                fetchLocations(grid)
                    .then(locs => {
                        setLocationsByGrid(prev => new Map([...prev, [grid, locs]]))
                        setLocationsLoadingGrids(prev => { const s = new Set(prev); s.delete(grid); return s })
                    })
                    .catch(err => {
                        console.error(`Failed to load locations for ${grid}:`, err)
                        setLocationsLoadingGrids(prev => { const s = new Set(prev); s.delete(grid); return s })
                    })
            }
        }
    }, [configs])  // eslint-disable-line react-hooks/exhaustive-deps

    const fetchedRef = useRef<Map<number, string>>(new Map())

    useEffect(() => {
        for (const config of configs) {
            if (!config.node || !sharedDate) continue

            const fetchKey = `${config.grid}|${config.node}|${sharedDate}`
            if (fetchedRef.current.get(config.id) === fetchKey) continue
            fetchedRef.current.set(config.id, fetchKey)

            setSeriesData(prev => new Map([...prev, [config.id, { data: [], loading: true, error: null }]]))

            fetchTimeseries(config.grid, config.node, sharedDate)
                .then(data => {
                    setSeriesData(prev => new Map([...prev, [config.id, { data, loading: false, error: null }]]))
                })
                .catch(err => {
                    setSeriesData(prev => new Map([...prev, [config.id, {
                        data: [],
                        loading: false,
                        error: err instanceof Error ? err.message : 'Unknown error',
                    }]]))
                })
        }
    }, [configs, sharedDate])

    function updateConfig(id: number, patch: Partial<SeriesConfig>) {
        setConfigs(prev => prev.map(c => c.id === id ? { ...c, ...patch } : c))
        if (patch.grid || patch.node) {
            fetchedRef.current.delete(id)
        }
    }

    function addSeries() {
        setConfigs(prev => [...prev, { id: nextId++, grid: 'ERCOT', node: '' }])
    }

    function removeSeries(id: number) {
        setConfigs(prev => prev.filter(c => c.id !== id))
        setSeriesData(prev => { const m = new Map(prev); m.delete(id); return m })
        fetchedRef.current.delete(id)
    }

    function clearOverlays() {
        const primary = configs[0]
        setConfigs([primary])
        setSeriesData(prev => {
            const m = new Map<number, SeriesData>()
            if (prev.has(primary.id)) m.set(primary.id, prev.get(primary.id)!)
            return m
        })
        fetchedRef.current = new Map([[primary.id, fetchedRef.current.get(primary.id) ?? '']])
    }

    function handleDateChange(date: string) {
        setSharedDate(date)
        // Invalidate all fetched keys so every series re-fetches for the new date
        fetchedRef.current.clear()
    }

    const readySeries = configs
        .map(c => ({ config: c, data: seriesData.get(c.id)?.data ?? [] }))
        .filter(s => s.data.length > 0)

    const chartData = buildChartData(readySeries)
    const anyLoading = configs.some(c => seriesData.get(c.id)?.loading)

    return (
        <div>
            <h2>LMP Timeseries</h2>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px', marginBottom: '16px' }}>
                {configs.map((config, i) => (
                    <SeriesSelector
                        key={config.id}
                        config={config}
                        locations={locationsByGrid.get(config.grid) ?? []}
                        locationsLoading={locationsLoadingGrids.has(config.grid)}
                        onChange={patch => updateConfig(config.id, patch)}
                        onRemove={i > 0 ? () => removeSeries(config.id) : undefined}
                        color={LINE_COLORS[i % LINE_COLORS.length]}
                        label={i === 0 ? 'Series 1' : `Series ${i + 1}`}
                    />
                ))}

                <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                    <label>
                        Date{' '}
                        <input
                            type='date'
                            value={sharedDate}
                            onChange={e => handleDateChange(e.target.value)}
                        />
                    </label>
                </div>
            </div>

            <div style={{ display: 'flex', gap: '8px', marginBottom: '24px' }}>
                <button onClick={addSeries} disabled={configs.length >= LINE_COLORS.length}>
                    + Add series
                </button>
                {configs.length > 1 && (
                    <button onClick={clearOverlays}>Clear overlays</button>
                )}
            </div>

            {anyLoading && <p>Loading timeseries...</p>}

            {configs.map(c => {
                const s = seriesData.get(c.id)
                return s?.error ? <p key={c.id} style={{ color: 'red' }}>Error: {s.error}</p> : null
            })}

            {chartData.length > 0 && (
                <ResponsiveContainer width='100%' height={300}>
                    <LineChart data={chartData} margin={{ top: 4, right: 24, left: 0, bottom: 4 }}>
                        <CartesianGrid strokeDasharray='3 3' />
                        <XAxis
                            dataKey='time_utc'
                            tickFormatter={key => utcKeyToLocalTime(key as string, sharedDate)}
                            minTickGap={40}
                        />
                        <YAxis
                            label={{ value: '$/MWh', angle: -90, position: 'insideLeft', offset: 10 }}
                        />
                        <Tooltip
                            labelFormatter={key => utcKeyToLocalTime(key as string, sharedDate)}
                            formatter={(value, name) => {
                                const nameStr = String(name)
                                const idx = configs.findIndex(c => `lmp_${c.id}` === nameStr)
                                const config = configs[idx]
                                const label = config ? `${config.node} (${config.grid})` : nameStr
                                return [`${Number(value).toFixed(2)} $/MWh`, label]
                            }}
                        />
                        <Legend
                            formatter={(_value, entry) => {
                                const name = entry.dataKey as string
                                const idx = configs.findIndex(c => `lmp_${c.id}` === name)
                                const config = configs[idx]
                                return config ? `${config.node} (${config.grid})` : name
                            }}
                        />
                        {configs.map((config, i) => (
                            <Line
                                key={config.id}
                                type='monotone'
                                dataKey={`lmp_${config.id}`}
                                dot={false}
                                stroke={LINE_COLORS[i % LINE_COLORS.length]}
                                strokeWidth={2}
                                connectNulls={true}
                            />
                        ))}
                    </LineChart>
                </ResponsiveContainer>
            )}
        </div>
    )
})
