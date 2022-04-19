from flask import Flask, Response
from pykafka import KafkaClient


def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')


app = Flask(__name__)


@app.route("/")
def index():
    return 'test'


@app.route('/topic/<topicname>')
def get_messages(topicname):
    client = get_kafka_client()

    def events():
        for i in client.topics[topicname].get_simple_consumer():
            yield '\n'.format(i.value.decode())

    return Response(events(), mimetype="text/event-stream")


if __name__ == "__main__":
    app.run(debug=True, port=5002)
