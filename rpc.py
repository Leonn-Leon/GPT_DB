from gpt_db.agent import GPTAgent
import pika
import os
import json

agent = GPTAgent()

url= os.getenv("RMQ_URL")
exchange = os.getenv("RMQ_EXCHANGE_NAME")
queue = os.getenv("RMQ_INPUT_QUEUE")
routing_key = os.getenv("RMQ_ROUTING_KEY")

connection = pika.BlockingConnection(pika.URLParameters(url))
channel = connection.channel()
channel.queue_declare(queue=queue, durable=True) #passive=True
channel.exchange_declare(exchange=exchange,  exchange_type="topic", durable=True) #passive=True
channel.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key)

def callback(ch, method, props, body):
    request_data = json.loads(body.decode())
    user_id = request_data.get("user_id", "default_user")
    message = request_data.get("message", "")

    response_ai = agent.run(user_id=user_id, message=message)
    response = response_ai.get("message")[-1].get('content')
    print('!!', response )

    ch.basic_publish(exchange='',
                     routing_key='getMessage.result', #props.reply_to,
                     properties=pika.BasicProperties(
                                        correlation_id = props.correlation_id, 
                                        headers={'rabbitmq_resp_correlationId': props.headers.get('rabbitmq_correlationId', '')}),
                     body=json.dumps(response).encode('utf-8'))
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue, on_message_callback=callback)

print(" [x] Awaiting RPC requests")
channel.start_consuming()