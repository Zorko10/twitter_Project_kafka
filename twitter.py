from tweepy import Stream, OAuthHandler
import credentials
import tweepy as tw
from pykafka import KafkaClient
import json

# Twitter API credentials
consumer_key = credentials.API_KEY
consumer_secret = credentials.API_SECRET_KEY
access_key = credentials.ACCESS_TOKEN
access_secret = credentials.ACCESS_TOKEN_SECRET


def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')   # se connecter à Kafka


class IDPrinter(tw.Stream):
    def on_data(self, data):
        print(data)
        #mystring = repr(data)
        #message = json.loads(data)                      # transformer data en json
        #if message['place'] is not None:               # collecte data seuleument si place existe
        client = get_kafka_client()                 # config kafka
        topic = client.topics['luffy']            # config kafka
        producer = topic.get_sync_producer()        # config kafka
        #producer.produce(mystring.encode('ascii'))      # config kafka
        #print(data):
        producer.produce(data)
        return True

    def on_error(self, status):
        print(status)



if __name__ == "__main__":
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tw.API(auth, wait_on_rate_limit=True)
    printer = IDPrinter(
        consumer_key,consumer_secret,access_key,access_secret

    )
    printer.filter(track=['elections'])