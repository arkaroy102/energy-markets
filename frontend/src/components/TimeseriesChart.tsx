import { useEffect, useState } from 'react'
import {
    LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer
} from 'recharts'
import { fetchLocations, fetchTimeseries } from '../api/client'
import type { NodeLocation, PricePoint } from '../types/market'

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

export default function TimeseriesChart() {
    const [selectedGrid, setSelectedGrid] = useState('ERCOT')
    const [locations, setLocations] = useState<NodeLocation[]>([])
    const [selectedNode, setSelectedNode] = useState<string>('')
    const [selectedDate, setSelectedDate] = useState(todayLocal())

    const [timeseries, setTimeseries] = useState<PricePoint[]>([])
    const [tsLoading, setTsLoading] = useState(false)
    const [tsError, setTsError] = useState<string | null>(null)

    useEffect(() => {
        setSelectedNode('')
        setTimeseries([])
        fetchLocations(selectedGrid)
            .then(setLocations)
            .catch(err => console.error('Failed to load locations:', err))
    }, [selectedGrid])

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
        <div>
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
