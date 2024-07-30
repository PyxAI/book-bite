import logging
import sys
import pika

from book_dec.funcs import proc_book_image

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
channel.queue_declare(queue="books")
logger = logging.getLogger(name="Book_server")
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def save_to_s3(file):
    pass


def on_request(ch, method, props, body):
    f_name = "/tmp/tmp.jpg"
    with open(f_name, "wb") as f:
        f.write(body)
    save_to_s3(body)
    channel.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(" [x] Received image from queue")
    response = proc_book_image(body)
    logger.info("Sending results")
    ch.basic_publish(
        exchange="",
        routing_key=props.reply_to,
        properties=pika.BasicProperties(correlation_id=props.correlation_id),
        body=str(response),
    )


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue="books", on_message_callback=on_request)

logger.info(" [x] Awaiting RPC requests")
channel.start_consuming()
