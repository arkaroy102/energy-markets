while True:
    t0 = time.perf_counter()
    # Spin in a loop and poll API
    now = datetime.now(ct)
    start = (now - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
    end   = (now).strftime("%Y-%m-%dT%H:%M:%S")

    page_num = 1

    params = {
        "SCEDTimestampFrom": start,
        "SCEDTimestampTo": end,
        "page": page_num,
        "size": 19000,
    }

    headers = {
        "Authorization": f"Bearer {get_access_token()}",
        "Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY,
    }

    total_pages = 1

    while page_num <= total_pages:
        params["page"] = page_num

        t0_0 = time.perf_counter()
        data = get_ercot_data(base_url, headers, params)
        assert data != None, "Failed to get ercot data"
        t0_1 = time.perf_counter()
        #print(resp.request.url)

        #data = resp.json()
        if page_num == 1:
            #print(data['_meta'])
            total_pages = data['_meta']['totalPages']

        print(f"Page: {data['_meta']['currentPage']} out of {total_pages}, numrecords: {len(data['data'])}")
        if 'data' not in data:
            print(f"No results fetched")
            time.sleep(poll_period)
            continue
        new_busses = []
        for row in data['data']:
            assert len(row) == 4, f"Unexpected row: {row}"
            electrical_bus = row[2]
            if electrical_bus not in location_id_dict:
                # fetch location from db
                row = get_location_by_name(electrical_bus)
                if row:
                    try:
                        location_id_dict[row['node_name']] = row['node_id']
                    except:
                        print(f"row: {row}")
                else:
                    print(f"New bus found: {electrical_bus}")
                    new_busses.append(electrical_bus)
        for row in put_locations(new_busses):
            try:
                location_id_dict[row['node_name']] = row['node_id']
            except:
                print(f"Could not add row: {row}")
                print(f"Node map size: {len(location_id_dict)}")

        payload = [{"node_id" : location_id_dict[row[2]],
                    "timestamp_utc" : datetime.fromisoformat(row[0]).replace(tzinfo=ct).astimezone(timezone.utc).isoformat(),
                    "lmp" : row[3]} for row in data['data']]
        t0_2 = time.perf_counter()
        put_prices(payload)
        t0_3 = time.perf_counter()

        metrics["ercot_api"]["count"] += 1
        metrics["ercot_api"]["total"] += (t0_1 - t0_0)
        metrics["ercot_api"]["last"] = (t0_1 - t0_0)

        metrics["serialize_prices"]["count"] += 1
        metrics["serialize_prices"]["total"] += (t0_2 - t0_1)
        metrics["serialize_prices"]["last"] = (t0_2 - t0_1)

        metrics["write_price"]["count"] += 1
        metrics["write_price"]["total"] += (t0_3 - t0_2)
        metrics["write_price"]["last"] = (t0_3 - t0_2)

        page_num += 1
        t1 = time.perf_counter()

    metrics["total"]["count"] += 1
    metrics["total"]["total"] += (t1 - t0)
    metrics["total"]["last"] = (t1 - t0)

    for key in metrics:
        avg = metrics[key]["total"] / metrics[key]["count"]
        print(f"avg {key}: {avg}, last: {metrics[key]["last"]}, callcount: {metrics[key]["count"]}")
    time.sleep(poll_period)
