import { useState, useEffect, useCallback, useRef, memo } from 'react'
import Map, { Source, Layer } from 'react-map-gl/maplibre'
import type { MapRef, MapLayerMouseEvent } from 'react-map-gl/maplibre'
import type { CircleLayer, SymbolLayer } from 'maplibre-gl'
import type { GeoJSONSource } from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'

const MAP_STYLE = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json'
const POLL_MS = 5000
const GRIDS = ['ERCOT', 'NYISO']
const SOURCE_ID = 'nodes'
const UNCLUSTERED_ID = 'nodes-unclustered'
const CLUSTER_ID = 'nodes-cluster'
const CLUSTER_COUNT_ID = 'nodes-cluster-count'

interface MapNode {
    node_id: number
    node_name: string
    latitude: number
    longitude: number
    settlement_load_zone: string | null
    lmp: number | null
    zone_avg_lmp: number | null
}

interface TooltipState {
    node_name: string
    settlement_load_zone: string | null
    lmp: number | null
    zone_avg_lmp: number | null
    x: number
    y: number
}

function toGeoJSON(nodes: MapNode[]) {
    return {
        type: 'FeatureCollection' as const,
        features: nodes.map(n => {
            const hasPrices = n.lmp !== null && n.zone_avg_lmp !== null && n.zone_avg_lmp !== 0
            const divergence = hasPrices
                ? Math.max(-0.3, Math.min(0.3, (n.lmp! - n.zone_avg_lmp!) / Math.abs(n.zone_avg_lmp!)))
                : 0
            return {
                type: 'Feature' as const,
                geometry: { type: 'Point' as const, coordinates: [n.longitude, n.latitude] },
                properties: {
                    node_id: n.node_id,
                    node_name: n.node_name,
                    settlement_load_zone: n.settlement_load_zone,
                    lmp: n.lmp,
                    zone_avg_lmp: n.zone_avg_lmp,
                    divergence,
                    has_price: hasPrices,
                },
            }
        }),
    }
}

const unclusteredLayer: CircleLayer = {
    id: UNCLUSTERED_ID,
    type: 'circle',
    source: SOURCE_ID,
    filter: ['!', ['has', 'point_count']],
    paint: {
        'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            3, 3,
            7, 5,
            12, 10,
        ],
        'circle-color': [
            'case',
            ['!', ['get', 'has_price']],
            '#666666',
            [
                'interpolate', ['linear'], ['get', 'divergence'],
                -0.3, '#2166ac',
                -0.05, '#74add1',
                0,    '#aaaaaa',
                0.05, '#f4a582',
                0.3,  '#d6604d',
            ],
        ],
        'circle-stroke-width': 1,
        'circle-stroke-color': '#222222',
        'circle-opacity': 0.9,
    },
}

const clusterLayer: CircleLayer = {
    id: CLUSTER_ID,
    type: 'circle',
    filter: ['has', 'point_count'],
    paint: {
        'circle-color': [
            'step', ['get', 'point_count'],
            '#4e9af1',
            10, '#f1c40f',
            30, '#e67e22',
        ],
        'circle-radius': ['step', ['get', 'point_count'], 18, 10, 26, 30, 36],
        'circle-opacity': 0.85,
        'circle-stroke-width': 2,
        'circle-stroke-color': '#ffffff',
    },
}

const clusterCountLayer: SymbolLayer = {
    id: CLUSTER_COUNT_ID,
    type: 'symbol',
    source: SOURCE_ID,
    filter: ['has', 'point_count'],
    layout: {
        'text-field': '{point_count_abbreviated}',
        'text-font': ['Noto Sans Regular'],
        'text-size': 12,
    },
    paint: {
        'text-color': '#ffffff',
    },
}

export default memo(function MapView() {
    const [nodes, setNodes] = useState<MapNode[]>([])
    const [tooltip, setTooltip] = useState<TooltipState | null>(null)
    const [cursor, setCursor] = useState('auto')
    const mapRef = useRef<MapRef>(null)

    useEffect(() => {
        let cancelled = false
        const load = async () => {
            try {
                const results = await Promise.all(
                    GRIDS.map(g => fetch(`/api/prices/map-nodes?grid=${g}`).then(r => r.ok ? r.json() : []))
                )
                if (!cancelled) setNodes(results.flat())
            } catch {
                // retain stale data on network error
            }
        }
        load()
        const id = setInterval(load, POLL_MS)
        return () => { cancelled = true; clearInterval(id) }
    }, [])

    const onMouseMove = useCallback((e: MapLayerMouseEvent) => {
        const features = e.features
        if (features && features.length > 0 && features[0].layer.id === UNCLUSTERED_ID) {
            const p = features[0].properties as Record<string, unknown>
            setCursor('pointer')
            setTooltip({
                node_name: p.node_name as string,
                settlement_load_zone: p.settlement_load_zone as string | null,
                lmp: p.lmp as number | null,
                zone_avg_lmp: p.zone_avg_lmp as number | null,
                x: e.point.x,
                y: e.point.y,
            })
        } else {
            setCursor(features && features.length > 0 ? 'pointer' : 'auto')
            setTooltip(null)
        }
    }, [])

    const onClick = useCallback((e: MapLayerMouseEvent) => {
        const features = e.features
        if (!features || features.length === 0) return
        const f = features[0]
        if (f.layer.id === CLUSTER_ID && mapRef.current) {
            const source = mapRef.current.getSource(SOURCE_ID) as GeoJSONSource
            const clusterId = (f.properties as Record<string, number>).cluster_id
            const coords = (f.geometry as GeoJSON.Point).coordinates as [number, number]
            source.getClusterExpansionZoom(clusterId).then(zoom => {
                mapRef.current?.flyTo({ center: coords, zoom, duration: 600 })
            })
        }
    }, [])

    const geoJSON = toGeoJSON(nodes)

    return (
        <div style={{ position: 'relative', height: 480, borderRadius: 8, overflow: 'hidden', cursor }}>
            <Map
                ref={mapRef}
                initialViewState={{ longitude: -90, latitude: 38, zoom: 4 }}
                style={{ width: '100%', height: '100%' }}
                mapStyle={MAP_STYLE}
                interactiveLayerIds={[UNCLUSTERED_ID, CLUSTER_ID]}
                onMouseMove={onMouseMove}
                onMouseLeave={() => { setTooltip(null); setCursor('auto') }}
                onClick={onClick}
            >
                <Source
                    id={SOURCE_ID}
                    type="geojson"
                    data={geoJSON}
                    cluster={false}
                >
                    <Layer {...unclusteredLayer} />
                </Source>
            </Map>

            {/* Divergence legend */}
            <div style={{
                position: 'absolute', bottom: 28, right: 10,
                background: 'rgba(20,20,20,0.85)', color: '#ccc',
                padding: '8px 12px', borderRadius: 6, fontSize: 11,
                pointerEvents: 'none',
            }}>
                <div style={{ marginBottom: 4, fontWeight: 600, color: '#eee' }}>LMP vs zone avg</div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ color: '#2166ac' }}>●</span>
                    <div style={{
                        width: 80, height: 8, borderRadius: 4,
                        background: 'linear-gradient(to right, #2166ac, #aaaaaa, #d6604d)',
                    }} />
                    <span style={{ color: '#d6604d' }}>●</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 3 }}>
                    <span>−30%</span><span>+30%</span>
                </div>
            </div>

            {/* Hover tooltip */}
            {tooltip && (
                <div style={{
                    position: 'absolute',
                    left: tooltip.x + 14,
                    top: tooltip.y - 14,
                    background: 'rgba(15,15,15,0.92)',
                    color: '#e8e8e8',
                    padding: '10px 14px',
                    borderRadius: 6,
                    pointerEvents: 'none',
                    fontSize: 12,
                    lineHeight: 1.6,
                    border: '1px solid rgba(255,255,255,0.12)',
                    maxWidth: 220,
                    zIndex: 10,
                }}>
                    <div style={{ fontWeight: 700, marginBottom: 2 }}>{tooltip.node_name}</div>
                    {tooltip.settlement_load_zone && (
                        <div style={{ color: '#aaa' }}>Zone: {tooltip.settlement_load_zone}</div>
                    )}
                    {tooltip.lmp != null
                        ? <div>LMP: <b>${tooltip.lmp.toFixed(2)}</b>/MWh</div>
                        : <div style={{ color: '#888' }}>No price data</div>
                    }
                    {tooltip.lmp != null && tooltip.zone_avg_lmp != null && (
                        <>
                            <div>Zone avg: ${tooltip.zone_avg_lmp.toFixed(2)}/MWh</div>
                            <div style={{
                                color: tooltip.lmp > tooltip.zone_avg_lmp ? '#f4a582' : '#74add1',
                                fontWeight: 600,
                            }}>
                                {((tooltip.lmp - tooltip.zone_avg_lmp) / Math.abs(tooltip.zone_avg_lmp) * 100).toFixed(1)}% vs avg
                            </div>
                        </>
                    )}
                </div>
            )}
        </div>
    )
})
