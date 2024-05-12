import json

_ATHENA_CLIENT = None

# Carregar schema única vez
with open('desafios/exercicio2/schema.json') as file:
    schema = json.load(file)

def create_hive_table_with_athena(query):
    '''
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    '''
    
    print(f"Query: {query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )

def validate_parameters(schema, bucket_name, folder, db_name, partition, tbl_name, file_format):
    """Validação de parametros"""
    if not isinstance(schema, dict):
        raise ValueError("Schema deve ser um dicionário")
    
    if not isinstance(bucket_name, str):
        raise ValueError("Bucket deve ser string")
    
    if not isinstance(folder, str):
        raise ValueError("Folder deve ser string")
    
    if not isinstance(db_name, str):
        raise ValueError("Database name deve ser string")
    
    if not isinstance(partition, str):
        raise ValueError("Partition deve ser string")
    
    if not isinstance(tbl_name, str) or not tbl_name:
        raise ValueError("Table name deve ser string não vazia")
    
    if partition != '' and partition not in schema['required']:
        raise ValueError(f"Partition '{partition}' precisa estar nos requireds do schema")

def format_fields(schema, new_line, sep):
    """Formatando campos necessário para construção da tabela"""
    new_line = new_line + 1
    space = new_line * "   "
    fields_list = []

    for key, value in schema['properties'].items():
        if value['type'] == 'object':
            fields_list.append("{space}{key} struct<\n{fields}>,\n".format(space=space, key=key, fields=format_fields(value, new_line, ':')))
        else:
            fields_list.append(f"{space}{key}{sep} {value['type']},\n")
    return ''.join(fields_list)[:-2]

def query_constructor(schema, bucket_name='', folder='', db_name='', partition='', tbl_name='', file_format=''):
    """Função responsável pela construção da query"""
    validate_parameters(schema, bucket_name, folder, db_name, partition, tbl_name, file_format)
    
    file_format_query = f" STORED AS {file_format}" if file_format else " STORED AS TEXTFILE"
    folder_query = f" LOCATION 's3://{bucket_name}/{folder}/'" if folder else f" LOCATION 's3://{bucket_name}/'"
    partition_query = f" PARTITIONED BY ({partition})" if partition else ""

    destination = f"{db_name}.{tbl_name}" if db_name else tbl_name

    fields_formatted = format_fields(schema, 0, '')

    query_parts = [
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {destination} (",
        fields_formatted,
        ")",
        f" ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'",
        partition_query,
        file_format_query,
        folder_query
    ]

    return '\n'.join(query_parts)

def handler():
    '''
    #  Função principal
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função create_hive_table_with_athena para te auxiliar
        na criação da tabela HIVE, não é necessário alterá-la
    '''


    query = query_constructor(schema, bucket_name='teste', folder='abc', db_name='', partition='', tbl_name='a', file_format='PARQUET')

    create_hive_table_with_athena(query)