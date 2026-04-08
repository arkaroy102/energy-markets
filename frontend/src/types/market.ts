export type ZonePrice = {
    settlement_load_zone: string
    avg_lmp: number
    min_timestamp_utc : number
    max_timestamp_utc : number
    num_nodes : number
}

export type NodeLocation = {
    node_id: number
    node_name: string
}

export type PricePoint = {
    timestamp_utc: string
    lmp: number
}
