import logging
import uuid
import sys
import time
import pika
from fastapi import FastAPI, Request, UploadFile, File
from starlette.responses import JSONResponse
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class RPCClient:
    """
    I want to send the message to a free server out of n servers.
    Wait until the message is acknowledged, which in that time, the file is saved to S3, and only then acked.
    The client will listen on
    """
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue="books_reply")
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            auto_ack=False,
            on_message_callback=self.on_response
        )

        self.response = None
        self.corr_id = None
        self.logger = logging.getLogger(name=self.__class__.__name__)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    async def call(self, image):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.logger.info("Sending request in rabbit")
        self.channel.basic_publish(
            exchange="",
            routing_key="books",
            properties=pika.BasicProperties(
                correlation_id=self.corr_id,
                reply_to=self.callback_queue
            ),
            body=image
        )
        while self.response is None:
            self.connection.process_data_events(time_limit=5)
            if not self.response:
                return ""
        self.logger.info("returning results!")
        return self.response.decode("utf-8")


app = FastAPI()
rpc = RPCClient()


@app.post("/identify")
async def identify(request: Request, file: UploadFile = File(...)):
    start = time.time()
    logging.info("got request")
    file_download = await file.read()
    resp = await rpc.call(file_download)
    end = time.time()
    return JSONResponse(
        {
            "book_id": str(resp),
            "elapsed_time": end - start
        }
    )


@app.get("/test")
async def my_test(request: Request):
    return JSONResponse(
        {"who?": "mee"}
    )
