#from gpt_db.agent import GPTAgent
from agent_ver2 import GPTAgent 
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
channel.queue_declare(queue=queue, durable=True) 
channel.exchange_declare(exchange=exchange,  exchange_type="topic", durable=True)
channel.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key)

def callback(ch, method, props, body):
    try:
        body_decode = body.decode('utf-8')
        request_data = json.loads(body_decode)
    except Exception as e:
        print('Ошибка! ', type(e).__name__)
        request_data = {}

    user_id = request_data.get("user_id", "default_user")
    message = request_data.get("query_text", "привет")  

    try:
        response_ai = agent.run(user_id=user_id, message=message)
    except Exception as e:
        response_ai = {'answer': type(e).__name__}
    
    type = 'CLARIFICATION_QUESTION' if response_ai.get("sql", '') == '' else 'FINAL_ANSWER'
    response = {
        #'content' : response_ai.get("messages")[-1].content,
        'question' : response_ai.get("question", ''),   #только в агенте2
        'answer' : response_ai.get("answer", ''),   #только в агенте2
        'sql_query' : response_ai.get("sql", ''),
        'comment' : response_ai.get("comment", ''),
        'user_id' : response_ai.get("user_id", ''),
        'type' : type
        }
    print('Полный ответ:\n', response)

    ch.basic_publish(exchange='', 
                     routing_key=props.reply_to, 
                     properties=pika.BasicProperties(
                                        correlation_id = props.correlation_id, 
                                        headers={'rabbitmq_resp_correlationId': props.headers.get('rabbitmq_correlationId', '')},
                                        content_type='application/json',
                                        content_encoding='utf-8'
                                        ),
                     body=json.dumps(response)
                     )
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue, on_message_callback=callback)

print(" [x] Awaiting RPC requests")
channel.start_consuming()