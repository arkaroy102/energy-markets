import ZonePriceTable from './components/ZonePriceTable'
import TimeseriesChart from './components/TimeseriesChart'

export default function App() {
    return (
        <div style={{ padding: '24px', fontFamily: 'sans-serif' }}>
            <ZonePriceTable />
            <hr style={{ margin: '32px 0' }} />
            <TimeseriesChart />
        </div>
    )
}
