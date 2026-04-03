import { useEffect, useState } from 'react'
import { fetchLatestZonePrices } from './api/client'
import type { ZonePrice } from './types/market'

export default function App() {
    const [rows, setRows] = useState<ZonePrice[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [lastRefreshed, setLastRefreshed] = useState<Date | null>(null)
    const [now, setNow] = useState(new Date())

    useEffect(() => {
        const clockId = window.setInterval(() => {
            setNow(new Date())
        }, 1000)

        return () => {
            window.clearInterval(clockId)
        }
    }, [])

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
                await load()                 // wait for request to finish
                await new Promise(r => setTimeout(r, 5000))
            }
        }

        loop()

        return () => {
            cancelled = true
        }
    }, [])

    return (
        <div style={{ padding: '24px', fontFamily: 'sans-serif' }}>
            <h1>Ercot Zone Prices</h1>

            <div style={{ marginBottom: '16px' }}>
            <div>Current time: {now.toLocaleString()}</div>
            <div>
        Last refreshed:{' '}
        {lastRefreshed ? lastRefreshed.toLocaleTimeString() : 'Never'}
        </div>
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
        </div>
    )
}
