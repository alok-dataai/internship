from google.cloud import pubsub_v1
import json
import time

project_id="playground-s-11-1a274b22"
topic_id="raw-telecom-data"
publisher=pubsub_v1.PublisherClient()
topic_path=publisher.topic_path(project_id,topic_id)

with open("telecom_data.json") as  f:
	data=json.load(f)

for record in data:
    message=json.dumps(record).encode("utf-8")
    publisher.publish(topic_path,data=message)
    time.sleep(1)
