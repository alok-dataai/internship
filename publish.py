from google.cloud import pubsub_v1
import json
import time
import google.auth

_,project_id=google.auth.default()
topic_id="raw-telecom-data"
publisher=pubsub_v1.PublisherClient()
topic_path=publisher.topic_path(project_id,topic_id)

with open("product_data.json") as  f:
	data=json.load(f)

for record in data:
    message=json.dumps(record).encode("utf-8")
    future=publisher.publish(topic_path,data=message)
    print(f"Published message {future.result()}")
    time.sleep(1)
