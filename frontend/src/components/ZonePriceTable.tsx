import { useEffect, useState } from 'react'
import { fetchLatestZonePrices } from '../api/client'
import type { ZonePrice } from '../types/market'

interface Props {
    grid: string
    title: string
    onRefresh?: (time: Date) => void
}

export default function ZonePriceTable({ grid, title, onRefresh }: Props) {
    const [rows, setRows] = useState<ZonePrice[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        let cancelled = false

        async function load() {
            try {
                const data = await fetchLatestZonePrices(grid)
                if (!cancelled) {
                    setRows(data)
                    setError(null)
                    setLoading(false)
                    onRefresh?.(new Date())
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
    }, [grid])

    return (
        <div>
            <h2>{title}</h2>

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
