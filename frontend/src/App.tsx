import { memo, useEffect, useState } from 'react'
import ZonePriceTable from './components/ZonePriceTable'
import TimeseriesChart from './components/TimeseriesChart'
import MapView from './components/MapView'

const Clock = memo(function Clock({ lastRefreshed }: { lastRefreshed: Date | null }) {
    const [now, setNow] = useState(new Date())

    useEffect(() => {
        const id = window.setInterval(() => setNow(new Date()), 1000)
        return () => window.clearInterval(id)
    }, [])

    return (
        <div style={{ marginBottom: '24px' }}>
            <div>Current time: {now.toLocaleString()}</div>
            <div>Last refreshed: {lastRefreshed ? lastRefreshed.toLocaleTimeString() : 'Never'}</div>
        </div>
    )
})

export default function App() {
    const [lastRefreshed, setLastRefreshed] = useState<Date | null>(null)

    return (
        <div style={{ padding: '24px', fontFamily: 'sans-serif' }}>
            <h1>Real-Time LMP Dashboard</h1>
            <Clock lastRefreshed={lastRefreshed} />

            <MapView />
            <hr style={{ margin: '32px 0' }} />

            <ZonePriceTable grid="ERCOT" title="ERCOT Zone Prices" onRefresh={setLastRefreshed} />
            <hr style={{ margin: '32px 0' }} />
            <ZonePriceTable grid="NYISO" title="NYISO Zone Prices" onRefresh={setLastRefreshed} />
            <hr style={{ margin: '32px 0' }} />
            <TimeseriesChart />
        </div>
    )
}
