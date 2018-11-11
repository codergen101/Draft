import csv
import json

csvfile = open('flows.csv', 'r')
jsonfile = open('flows.json', 'w')

fieldnames = ("count", "ip_protocol", "destination_port", "source_group_name", "source_asset_name",
              "source_process_name", "source_ip_addresses", "destination_group_name", "destination_asset_name",
              "destination_process_name", "destination_ip_addresses")

reader = csv.DictReader(csvfile, fieldnames)

for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write('\n')

jsonfile.close()
csvfile.close()
