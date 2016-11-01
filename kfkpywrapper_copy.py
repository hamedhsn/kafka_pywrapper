import time
import logging
import io
import json

import dm.config.kafka as kfkcfg
from pykafka import KafkaClient
from bson import json_util
from avro import schema
from avro.io import BinaryDecoder, BinaryEncoder, DatumWriter, DatumReader
from pykafka.common import OffsetType

__author__ = 'Hamed'


class DMKafka:
    def __init__(self, kfkdeb, ser_type, avro_schema_dict,
                 kfka_typ, ktopic=None, con_grp_nm=None):

        self.depkfk = kfkdeb
        self.ser_type = ser_type

        if ser_type == kfkcfg.SERIALIZATIO_AVRO:
            try:
                self.avro_schema = schema.Parse(json.dumps(avro_schema_dict))
            except:
                raise Exception('Problem with parsing your avro schema. Check your schema!')

        num_try = 0
        while True:
            try:
                if kfka_typ == kfkcfg.KFK_PRODUCER:
                    self.kfkprod = KafkaProducer(bootstrap_servers=self.depkfk)
                    logging.info('Created kafka producer :)')

                elif kfka_typ == kfkcfg.KFK_CONSUMER:
                    self.kfkcon = KafkaConsumer(ktopic, group_id=con_grp_nm, bootstrap_servers=self.depkfk)
                    logging.info('Created kafka Consumer :)')
            except:
                if num_try == 0:
                    logging.info('Failed to connect to kafka!')
                    exit()
                num_try += 1
                continue
            break


class DMProducer(DMKafka):
    def __init__(self, kfkdeb=kfkcfg.KAFKA_CLUSTR,
                 ser_type=kfkcfg.SERIALIZATIO_JSON,
                 avro_schema_dict=None):

        super().__init__(kfkdeb=kfkdeb, ser_type=ser_type,
                         avro_schema_dict=avro_schema_dict,
                         kfka_typ=kfkcfg.KFK_PRODUCER)
        # self.kfkprod = KafkaProducer(bootstrap_servers=self.depkfk)

    def produce(self, ktopic, msg):
        if self.ser_type == kfkcfg.SERIALIZATIO_JSON:

            msg = json.dumps(msg, default=json_util.default).encode('utf-8')
            future = self.kfkprod.send(ktopic, msg)

        elif self.ser_type == kfkcfg.SERIALIZATIO_AVRO:

            writer = DatumWriter(self.avro_schema)
            bytes_writer = io.BytesIO()
            encoder = BinaryEncoder(bytes_writer)
            writer.write(msg, encoder)
            raw_bytes = bytes_writer.getvalue()

            future = self.kfkprod.send(ktopic, raw_bytes)
            # future = self.kfkprod.send(ktopic, msg)

        try:
            time.sleep(0.1)
            record_metadata = future.get(timeout=15)
        except Exception as e:
            logging.warning(e)


class DMConsumer(DMKafka):
    def __init__(self, ktopic, con_grp_nm,
                 kfkdeb=kfkcfg.KAFKA_CLUSTR,
                 ser_type=kfkcfg.SERIALIZATIO_JSON,
                 avro_schema_dict=None,
                 print_msg=False):

        super().__init__(kfkdeb=kfkdeb, ser_type=ser_type,
                         avro_schema_dict=avro_schema_dict,
                         ktopic=ktopic,
                         kfka_typ=kfkcfg.KFK_CONSUMER,
                         con_grp_nm=con_grp_nm)

        # self.kfkconsumer = KafkaConsumer(ktopic, group_id=con_grp_nm,
        #                          bootstrap_servers=)
        # self.kfkconsumer.seek(0, 0)
        self.print_msg = print_msg

    def consume(self, *args, **kwargs):
        for message in self.kfkcon:
            try:
                if self.ser_type == kfkcfg.SERIALIZATIO_JSON:
                    message = json.loads(message.value.decode('utf-8'))
                elif self.ser_type == kfkcfg.SERIALIZATIO_AVRO:
                    bytes_reader = io.BytesIO(message.value)
                    decoder = BinaryDecoder(bytes_reader)
                    reader = DatumReader(self.avro_schema)
                    try:
                        message = reader.read(decoder)
                        print(message)
                    except Exception as e:
                        print(e)
                        pass

                if self.print_msg:
                    logging.info('Message to consume: {}'.format(message))
                    print('Message to consume: {}'.format(message))

                yield message

            except Exception as e:
                logging.info('unable to parse the msg!: {}...error: {}'.format(message, e))


''' EXAMPLE:
    import dm.utils.kafka as kfk

    ktopic = 'my-replicated-topic'
    # produce a json message
    a = kfk.DMProducer()
    a.produce(ktopic=ktopic, json_msg={'test':33})

    # consume a message
    kfk.DMConsumer(ktopic=ktopic, con_grp_nm=con_grp_nm).consume() # this will simply print out message
    # consume and by running a function on it
    kfk.Consumer(ktopic=ktopic, con_grp_nm=con_grp_nm).consume(func) # you should have defined your function pass it here

#### USING AVRO

## PRODUCE
from dm.utils.kafka import DMProducer, DMConsumer
import dm.config.avro as avrcfg
import dm.config.kafka as kfkcfg

kfkproducer = DMProducer(ser_type=kfkcfg.SERIALIZATIO_AVRO, avro_schema_dict=avrcfg.user)
ktopic = 'news-scraper-100'
for i in range(10):
    kfkproducer.produce(ktopic=ktopic, msg={"name": "Ben", "favorite_number": 7, "favorite_color": "red"})

## CONSUME
ktopic = 'news-scraper-100'
con_grp_nm = 'news1'
kfkconsumer = DMConsumer(ktopic=ktopic, con_grp_nm=con_grp_nm, ser_type=kfkcfg.SERIALIZATIO_AVRO,
                         avro_schema_dict=avrcfg.user)
kfkconsumer.consume()
'''


class DMPYKafka:
    def __init__(self, kfkdeb, ser_type, avro_schema_dict,
                 kfka_typ, ktopic, con_grp_nm=None,
                 read_from_begining=False, set_offset_to_end=False):

        self.depkfk = kfkdeb
        self.zkeepers = ', '.join([a.replace('9092', '2181') for a in self.depkfk])
        self.ser_type = ser_type

        if ser_type == kfkcfg.SERIALIZATIO_AVRO:
            try:
                self.avro_schema = schema.Parse(json.dumps(avro_schema_dict))
            except:
                raise Exception('Problem with parsing your avro schema. Check your schema!')

        num_try = 0
        while True:
            try:
                client = KafkaClient(hosts=', '.join(self.depkfk))
                topic = client.topics[bytes(ktopic, 'utf-8')]

                if kfka_typ == kfkcfg.KFK_PRODUCER:
                    self.kfkprod = topic.get_sync_producer(max_request_size=100001200)
                    logging.info('Created kafka producer :)')

                elif kfka_typ == kfkcfg.KFK_CONSUMER:
                    # self.kfkcon = topic.get_simple_consumer(consumer_group=bytes(con_grp_nm, 'utf-8'),
                    #                                      auto_commit_enable=True, auto_commit_interval_ms=5000)
                    if read_from_begining:
                        self.kfkcon = topic.get_balanced_consumer(consumer_group=bytes(con_grp_nm, 'utf-8'),
                                                                  zookeeper_connect=self.zkeepers,
                                                                  auto_offset_reset=OffsetType.EARLIEST,
                                                                  reset_offset_on_start=True,
                                                                  auto_commit_interval_ms=5000,
                                                                  auto_commit_enable=True)
                    elif set_offset_to_end:
                        self.kfkcon = topic.get_balanced_consumer(consumer_group=bytes(con_grp_nm, 'utf-8'),
                                                                  zookeeper_connect=self.zkeepers,
                                                                  auto_offset_reset=OffsetType.LATEST,
                                                                  reset_offset_on_start=True,
                                                                  auto_commit_interval_ms=5000,
                                                                  auto_commit_enable=True)
                    else:
                        self.kfkcon = topic.get_balanced_consumer(consumer_group=bytes(con_grp_nm, 'utf-8'),
                                                                  zookeeper_connect=self.zkeepers,
                                                                  auto_commit_interval_ms=5000,
                                                                  auto_commit_enable=True)

                    # self.kfkcon = KafkaConsumer(ktopic, group_id=con_grp_nm, bootstrap_servers=self.depkfk)
                    logging.info('Created kafka Consumer :)')
            except:
                if num_try == 0:
                    logging.info('Failed to connect to kafka!')
                    exit()
                num_try += 1
                continue
            break


class DMPYProducer(DMPYKafka):
    def __init__(self, ktopic, kfkdeb=kfkcfg.KAFKA_CLUSTR,
                 ser_type=kfkcfg.SERIALIZATIO_JSON,
                 avro_schema_dict=None):

        super().__init__(kfkdeb=kfkdeb, ser_type=ser_type,
                         avro_schema_dict=avro_schema_dict,
                         kfka_typ=kfkcfg.KFK_PRODUCER, ktopic=ktopic)
        # self.kfkprod = KafkaProducer(bootstrap_servers=self.depkfk)

    def produce(self, msg):
        if self.ser_type == kfkcfg.SERIALIZATIO_JSON:
            # s = json.dumps(msg)
            s = json.dumps(msg, default=json_util.default)
            self.kfkprod.produce(bytes(s, 'utf-8'))
            # msg = json.dumps(msg, default=json_util.default).encode('utf-8')
            # future = self.kfkprod.produce(bytes(msg))

        elif self.ser_type == kfkcfg.SERIALIZATIO_AVRO:

            writer = DatumWriter(self.avro_schema)
            bytes_writer = io.BytesIO()
            encoder = BinaryEncoder(bytes_writer)
            writer.write(msg, encoder)
            raw_bytes = bytes_writer.getvalue()

            future = self.kfkprod.produce(raw_bytes)
            # future = self.kfkprod.produce(ktopic, msg)


class DMPYConsumer(DMPYKafka):
    def __init__(self, ktopic, con_grp_nm,
                 kfkdeb=kfkcfg.KAFKA_CLUSTR,
                 ser_type=kfkcfg.SERIALIZATIO_JSON,
                 avro_schema_dict=None,
                 read_from_begining=False,
                 set_offset_to_end=False,
                 print_msg=False):

        super().__init__(kfkdeb=kfkdeb, ser_type=ser_type,
                         avro_schema_dict=avro_schema_dict,
                         ktopic=ktopic,
                         kfka_typ=kfkcfg.KFK_CONSUMER,
                         con_grp_nm=con_grp_nm,
                         read_from_begining=read_from_begining,
                         set_offset_to_end=set_offset_to_end)

        self.print_msg = print_msg

    def consume(self):
        for message in self.kfkcon:
            try:
                if self.ser_type == kfkcfg.SERIALIZATIO_JSON:
                    message = json.loads(message.value.decode('utf-8'))

                elif self.ser_type == kfkcfg.SERIALIZATIO_AVRO:
                    bytes_reader = io.BytesIO(message.value)
                    decoder = BinaryDecoder(bytes_reader)
                    reader = DatumReader(self.avro_schema)
                    try:
                        message = reader.read(decoder)
                        print(message)
                    except Exception as e:
                        print(e)
                        pass

                if self.print_msg:
                    logging.info('Message to consume: {}'.format(message))
                    print('Message to consume: {}'.format(message))

                yield message
            except Exception as e:
                logging.info('unable to parse the msg!: {}...error: {}'.format(message, e))
