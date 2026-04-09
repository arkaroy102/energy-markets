import { useEffect, useState } from 'react'
import ZonePriceTable from './components/ZonePriceTable'
import TimeseriesChart from './components/TimeseriesChart'

export default function App() {
    const [now, setNow] = useState(new Date())
    const [lastRefreshed, setLastRefreshed] = useState<Date | null>(null)

    useEffect(() => {
        const id = window.setInterval(() => setNow(new Date()), 1000)
        return () => window.clearInterval(id)
    }, [])

    return (
        <div style={{ padding: '24px', fontFamily: 'sans-serif' }}>
            <h1>Real-Time LMP Dashboard</h1>
            <div style={{ marginBottom: '24px' }}>
                <div>Current time: {now.toLocaleString()}</div>
                <div>Last refreshed: {lastRefreshed ? lastRefreshed.toLocaleTimeString() : 'Never'}</div>
            </div>

            <ZonePriceTable grid="ERCOT" title="ERCOT Zone Prices" onRefresh={setLastRefreshed} />
            <hr style={{ margin: '32px 0' }} />
            <ZonePriceTable grid="NYISO" title="NYISO Zone Prices" onRefresh={setLastRefreshed} />
            <hr style={{ margin: '32px 0' }} />
            <TimeseriesChart />
        </div>
    )
}
