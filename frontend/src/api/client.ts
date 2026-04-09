import type { ZonePrice, NodeLocation, PricePoint } from '../types/market'

export async function fetchLatestZonePrices(grid: string): Promise<ZonePrice[]> {
    const response = await fetch(`/api/prices/zone-summary?grid=${encodeURIComponent(grid)}`)

    if (!response.ok) {
        throw new Error(`Failed to fetch zone prices: ${response.status}`)
    }

    return response.json()
}

export async function fetchLocations(grid: string): Promise<NodeLocation[]> {
    const response = await fetch(`/api/locations?grid=${encodeURIComponent(grid)}`)

    if (!response.ok) {
        throw new Error(`Failed to fetch locations: ${response.status}`)
    }

    return response.json()
}

export async function fetchTimeseries(grid: string, nodeName: string, date: string): Promise<PricePoint[]> {
    const params = new URLSearchParams({ grid, node_name: nodeName, date })
    const response = await fetch(`/api/prices/timeseries?${params}`)

    if (!response.ok) {
        throw new Error(`Failed to fetch timeseries: ${response.status}`)
    }

    return response.json()
}
