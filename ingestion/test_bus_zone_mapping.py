import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

from ercot_client import _fetch_bus_to_zone

mapping = _fetch_bus_to_zone()
print(f"\nTotal mappings: {len(mapping)}")
print("Sample entries:")
for bus, zone in list(mapping.items())[:10]:
    print(f"  {bus} -> {zone}")
