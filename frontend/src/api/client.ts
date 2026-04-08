import type { ZonePrice, NodeLocation, PricePoint } from '../types/market'

export async function fetchLatestZonePrices(): Promise<ZonePrice[]> {
    const response = await fetch('/api/latest-zone-prices')

    if (!response.ok) {
        throw new Error(`Failed to fetch latest zone prices: ${response.status}`)
    }

    return response.json()
}

export async function fetchLocations(grid: string): Promise<NodeLocation[]> {
    const response = await fetch(`/locations?grid=${encodeURIComponent(grid)}`)

    if (!response.ok) {
        throw new Error(`Failed to fetch locations: ${response.status}`)
    }

    return response.json()
}

export async function fetchTimeseries(grid: string, nodeName: string, date: string): Promise<PricePoint[]> {
    const params = new URLSearchParams({ grid, node_name: nodeName, date })
    const response = await fetch(`/prices/timeseries?${params}`)

    if (!response.ok) {
        throw new Error(`Failed to fetch timeseries: ${response.status}`)
    }

    return response.json()
}
