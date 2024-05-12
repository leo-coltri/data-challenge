import json
import utils
import logging
import boto3

_SQS_CLIENT = None

## Configuração do logging
logging.basicConfig(filename='desafios/exercicio1/error.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Carregar schema única vez
with open('desafios/exercicio1/schema.json') as file:
    schema_validate = json.load(file)

def send_event_to_queue(event, queue_name):
    '''
     Responsável pelo envio do evento para uma fila
    :param event: Evento  (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    '''
    
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")

def handler(event):
    '''
    #  Função principal que é sensibilizada para cada evento
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função send_event_to_queue para envio do evento para a fila,
        não é necessário alterá-la
    '''

    # Extraindo campos do schema e do evento para validação
    list_fields_schema = utils.extract_schema_fields(schema_validate)
    list_fields_event = utils.extract_event_fields(event)

    # Salvando diferença entre campos
    in_events_not_schema = set(list_fields_event) - set(list_fields_schema)
    in_schema_not_events = set(list_fields_schema) - set(list_fields_event)

    event_id = event['eid']

    # Validações das diferenças dos campos
    if len(list_fields_event) != len(list_fields_schema):
        logging.error(f'event {event_id} - Schema com contagem de campos diferentes')

    if in_events_not_schema:
        logging.error(f'event {event_id} - Campos enviados que não constam no schema: {in_events_not_schema}')
    
    if in_schema_not_events:
        logging.error(f'event {event_id} - Campos do schema que não aparecem no evento: {in_schema_not_events}')

    # Extraindo tipos dos valores do evento e, dos valores de exemlo do schema.json
    dict_types_schema = utils.extract_schema_types(schema_validate)
    dict_types_event = utils.extract_event_types(event)

    # Verificação se os tipos das variáveis/valores dão match 
    for key, key_type in dict_types_schema.items():
        try:
            if dict_types_event[key] != key_type:
                logging.error(f'event {event_id} - Diferentes tipos dado schema e evento: {key}')
        except KeyError as ke:
            logging.error(f"event {event_id} - Não foi possivel encontrar a seguinte chave no evento: {ke}")
            continue
    
    # Extraindo ids de eventos no arquivo de logs de erros
    event_ids_error = utils.extract_event_ids_from_log('desafios/exercicio1/error.log')

    # Caso o evento não tenha aparecido no log podemos mandar ele para a fila valid-events-queue
    if event_id not in event_ids_error:
        send_event_to_queue(event, 'valid-events-queue')


