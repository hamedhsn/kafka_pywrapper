
KAFKA_CLUSTR = ['127.0.0.1:9092']

SERIALIZATIO_AVRO = 'avro'
SERIALIZATIO_JSON = 'json'

KFK_PRODUCER = 'Producer'
KFK_CONSUMER = 'Consumer'

# ##### AVRO Schema if Any #####
avro_test_schema = {
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number", "type": ["int", "null"]},
        {"name": "favorite_color", "type": ["string", "null"]}
    ]
}