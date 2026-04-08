import { useEffect, useState } from 'react'
import {
    LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer
} from 'recharts'
import { fetchLatestZonePrices, fetchLocations, fetchTimeseries } from './api/client'
import type { ZonePrice, NodeLocation, PricePoint } from './types/market'

const GRIDS = ['ERCOT', 'NYISO', 'CAISO']

function todayLocal(): string {
    // en-CA gives YYYY-MM-DD format in the browser's local timezone
    return new Date().toLocaleDateString('en-CA')
}

function formatTime(timestamp: string): string {
    return new Date(timestamp).toLocaleTimeString([], {
        hour: '2-digit',
        minute: '2-digit',
    })
}

export default function App() {
    // --- Zone price table ---
    const [rows, setRows] = useState<ZonePrice[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [lastRefreshed, setLastRefreshed] = useState<Date | null>(null)
    const [now, setNow] = useState(new Date())

    // --- Timeseries selector ---
    const [selectedGrid, setSelectedGrid] = useState('ERCOT')
    const [locations, setLocations] = useState<NodeLocation[]>([])
    const [selectedNode, setSelectedNode] = useState<string>('')
    const [selectedDate, setSelectedDate] = useState(todayLocal())

    // --- Timeseries chart ---
    const [timeseries, setTimeseries] = useState<PricePoint[]>([])
    const [tsLoading, setTsLoading] = useState(false)
    const [tsError, setTsError] = useState<string | null>(null)

    // Live clock
    useEffect(() => {
        const clockId = window.setInterval(() => setNow(new Date()), 1000)
        return () => window.clearInterval(clockId)
    }, [])

    // Zone price polling loop
    useEffect(() => {
        let cancelled = false

        async function load() {
            try {
                const data = await fetchLatestZonePrices()
                if (!cancelled) {
                    setRows(data)
                    setError(null)
                    setLastRefreshed(new Date())
                    setLoading(false)
                }
            } catch (err) {
                if (!cancelled) {
                    setError(err instanceof Error ? err.message : 'Unknown error')
                    setLoading(false)
                }
            }
        }

        async function loop() {
            while (!cancelled) {
                await load()
                await new Promise(r => setTimeout(r, 5000))
            }
        }

        loop()
        return () => { cancelled = true }
    }, [])

    // Load nodes when grid changes
    useEffect(() => {
        setSelectedNode('')
        setTimeseries([])
        fetchLocations(selectedGrid)
            .then(setLocations)
            .catch(err => console.error('Failed to load locations:', err))
    }, [selectedGrid])

    // Fetch timeseries when all three selectors are set
    useEffect(() => {
        if (!selectedNode || !selectedDate) return

        setTsLoading(true)
        setTsError(null)
        fetchTimeseries(selectedGrid, selectedNode, selectedDate)
            .then(data => {
                setTimeseries(data)
                setTsLoading(false)
            })
            .catch(err => {
                setTsError(err instanceof Error ? err.message : 'Unknown error')
                setTsLoading(false)
            })
    }, [selectedGrid, selectedNode, selectedDate])

    return (
        <div style={{ padding: '24px', fontFamily: 'sans-serif' }}>
            <h1>ERCOT Zone Prices</h1>

            <div style={{ marginBottom: '16px' }}>
                <div>Current time: {now.toLocaleString()}</div>
                <div>Last refreshed: {lastRefreshed ? lastRefreshed.toLocaleTimeString() : 'Never'}</div>
            </div>

            {loading && <p>Loading...</p>}
            {error && <p>Error: {error}</p>}

            {!loading && !error && (
                <table>
                    <thead>
                        <tr>
                            <th style={{ textAlign: 'left', paddingRight: '16px' }}>Zone</th>
                            <th style={{ textAlign: 'left', paddingRight: '16px' }}>Average LMP</th>
                            <th style={{ textAlign: 'left', paddingRight: '16px' }}>Min Node Time</th>
                            <th style={{ textAlign: 'left', paddingRight: '16px' }}>Max Node Time</th>
                            <th style={{ textAlign: 'left', paddingRight: '16px' }}>Num Nodes</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows.map((row) => (
                            <tr key={row.settlement_load_zone}>
                                <td style={{ paddingRight: '16px' }}>{row.settlement_load_zone}</td>
                                <td>{row.avg_lmp.toFixed(2)}</td>
                                <td>{new Date(row.min_timestamp_utc).toLocaleString()}</td>
                                <td>{new Date(row.max_timestamp_utc).toLocaleString()}</td>
                                <td>{row.num_nodes}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            )}

            <hr style={{ margin: '32px 0' }} />

            <h2>LMP Timeseries</h2>

            <div style={{ display: 'flex', gap: '16px', marginBottom: '24px', alignItems: 'center' }}>
                <label>
                    Grid{' '}
                    <select value={selectedGrid} onChange={e => setSelectedGrid(e.target.value)}>
                        {GRIDS.map(g => <option key={g} value={g}>{g}</option>)}
                    </select>
                </label>

                <label>
                    Node{' '}
                    <select
                        value={selectedNode}
                        onChange={e => setSelectedNode(e.target.value)}
                        disabled={locations.length === 0}
                    >
                        <option value=''>-- select node --</option>
                        {locations.map(loc => (
                            <option key={loc.node_id} value={loc.node_name}>{loc.node_name}</option>
                        ))}
                    </select>
                </label>

                <label>
                    Date{' '}
                    <input
                        type='date'
                        value={selectedDate}
                        onChange={e => setSelectedDate(e.target.value)}
                    />
                </label>
            </div>

            {tsLoading && <p>Loading timeseries...</p>}
            {tsError && <p>Error: {tsError}</p>}
            {!tsLoading && !tsError && timeseries.length === 0 && selectedNode && (
                <p>No data for this node on {selectedDate}.</p>
            )}

            {!tsLoading && timeseries.length > 0 && (
                <ResponsiveContainer width='100%' height={300}>
                    <LineChart data={timeseries} margin={{ top: 4, right: 24, left: 0, bottom: 4 }}>
                        <CartesianGrid strokeDasharray='3 3' />
                        <XAxis
                            dataKey='timestamp_utc'
                            tickFormatter={formatTime}
                            minTickGap={40}
                        />
                        <YAxis
                            label={{ value: '$/MWh', angle: -90, position: 'insideLeft', offset: 10 }}
                        />
                        <Tooltip
                            labelFormatter={formatTime}
                            formatter={(value: number) => [`${value.toFixed(2)} $/MWh`, 'LMP']}
                        />
                        <Line
                            type='monotone'
                            dataKey='lmp'
                            dot={false}
                            stroke='#2563eb'
                            strokeWidth={2}
                        />
                    </LineChart>
                </ResponsiveContainer>
            )}
        </div>
    )
}
