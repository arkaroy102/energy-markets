import type { ZonePrice } from '../types/market'

export async function fetchLatestZonePrices(): Promise<ZonePrice[]> {
    const response = await fetch('/api/latest-zone-prices')

    if (!response.ok) {
        throw new Error(`Failed to fetch latest zone prices: ${response.status}`)
    }

    return response.json()
}
