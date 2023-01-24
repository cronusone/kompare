import logging
import sys
import configparser
import click
from termcolor import colored
from elasticsearch_dsl import Search
import elasticsearch

CACHE = {}

logging.basicConfig(
    level=logging.CRITICAL,
    format="%(filename)s: "
           "%(levelname)s: "
           "%(funcName)s(): "
           "%(lineno)d:\t"
           "%(message)s",
)

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read("kompare.ini")


@click.group(invoke_without_command=True)
@click.option("--debug", is_flag=True, default=False, help="Debug switch")
@click.pass_context
def cli(context, debug):
    import boto3

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    if debug:
        logging.basicConfig(
            format="%(asctime)s : %(name)s %(levelname)s : %(message)s",
            level=logging.DEBUG,
        )
    else:
        logging.basicConfig(
            format="%(asctime)s : %(name)s %(levelname)s : %(message)s",
            level=logging.CRITICAL,
        )

    logging.debug("Debug ON")
    if len(sys.argv) == 1:
        click.echo(context.get_help())

    context.obj = {}
    context.obj['config'] = config

    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        
        context.obj['elastic'] = elasticsearch.Elasticsearch(config["elasticsearch"]["url"], verify_certs=False)
        context.obj['dynamodb'] = boto3.resource("dynamodb", endpoint_url=config["dynamodb"]["url"])


def check_elastic(elastic, index, eid, eid_value):
    
    if eid_value in CACHE:
        logging.debug("check_elastic: returning from cache %s", eid_value)
        return CACHE[eid_value]
    
    query = {
        'query': {
            'term' : {
            }
        }
    }
    query['query']['term'] = {}
    query['query']['term'][eid+".keyword"] = eid_value
    results = elastic.search(index=index, body=query)
    logging.debug("check_elastic: %s", results)
    logging.debug("FOUND %s hits", results['hits']['total']['value'])
    result = results['hits']['total']['value'] >= 1
    if result == 0:
        return False
    
    for r in results['hits']['hits']:
        logging.info(r['_source'][eid])
    result = results['hits']['hits'][0]['_source'][eid] == eid_value
    CACHE[eid_value] = result
        
    logging.debug("check_elastic: returning %s", result)
    return result

def check_dynamo(dynamodb, table, did, eid_value):
    from boto3.dynamodb.conditions import Key

    table = dynamodb.Table(table)
    response = table.query(
        KeyConditionExpression=Key(did).eq(eid_value)
    )

    return response['Count'] > 0


def scan(table, **kwargs):
    response = table.scan(**kwargs)
    yield from response['Items']
    while response.get('LastEvaluatedKey'):
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        yield from response['Items']

@cli.command(name="check")
@click.option("-f", "--field", required=False, help="Document id field name")
@click.option("-v", "--value", required=False, help="ID of document")
@click.option("-t", "--table", required=False, help="DynamoDB table")
@click.option("-i", "--index", required=False, help="ElasticSearch index")
@click.option("-o", "--out", required=True, default="kompare-check.csv", help="Name of output file")
@click.option("-in", "--infile", required=False, default=None, help="Scan output file containing dynamoids")
@click.option("-s", "--source", type=click.Choice(['elastic', 'dynamo'], case_sensitive=False), help="Database source")
@click.option("-s", "--sync", required=False, is_flag=True, default=False, help="Sync missing documents to elasticsearch")
@click.pass_context
def check(context, field, value, table, index, out, infile, source, sync):
    """Check if a document exists"""
    import csv
    import warnings
    from progress.bar import Bar

    with warnings.catch_warnings():
        warnings.simplefilter("ignore") 
        
        if infile and sync:
            with open(infile, 'r') as csvfile:
                reader = csv.reader(csvfile)
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")

                    for row in reader:
                        # [table, index, eid, did, did_value]
                        r_table = row[0]
                        r_index = row[1]
                        r_eid = row[2]
                        r_did = row[3]
                        r_value = row[4]
                        
                        dynamodb = context.obj['dynamodb']
                        _table = dynamodb.Table(r_table)
                        response = table.query(
                            KeyConditionExpression=Key(r_did).eq(r_value)
                        )
                        logging.info("%s", response)
                        
                        # Sync to elastic here
        
        
        if not infile:
            if source == 'elastic':
                print(check_elastic(context.obj['elastic'], index, field, value))
            if source == 'dynamo':
                print(check_dynamo(context.obj["dynamodb"], table,field,value))
        else:
            if infile:
                with open(infile, 'r') as idfile:
                    idcsv = csv.reader(idfile)
                    _total = sum(1 for row in idcsv)
                    
                with open(infile, 'r') as idfile:
                    idcsv = csv.reader(idfile)
                    
                    bar = Bar('Checking', max=_total)
                    idcsv = csv.reader(idfile)
                    logging.debug("file mode: %s",_total)
                    dynamo_misses = 0
                    
                    with open(out, 'w') as csvfile:
                        writer = csv.writer(csvfile)
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            for row in idcsv:
                                r_table = row[0]
                                r_index = row[1]
                                r_eid = row[2]
                                r_did = row[3]
                                r_value = row[4]
                                logging.debug("ROW %s",row)
                                logging.debug("Checking elastic")
                                if not check_elastic(context.obj['elastic'], r_index, r_eid, r_value):
                                    dynamo_misses += 1
                                    writer.writerow([r_index, r_eid,r_value])
                                    
                                bar.next()
              
    
@cli.command(name="scan")
@click.option("-did", required=True, help="DynamoDB document id field name")
@click.option("-t", "--table", required=True, help="DynamoDB table")
@click.option("-o", "--out", required=True, help="Name of output file")
@click.pass_context
def scan_ids(context, did, table, out):
    """ Scan dynamo table and store id's in a file """
    import boto3
    import warnings
    import csv
    from progress.bar import Bar

    dynamodb = context.obj['dynamodb']
    
    _table = dynamodb.Table(table)
    _total = _table.item_count
    bar = Bar('Scanning', max=_total)
    
    with open(out, 'w') as csvfile:

        writer = csv.writer(csvfile)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            seen = []
            for doc in scan(_table):
                did_value = doc[did]
                if did_value not in seen:
                    seen += [did_value]
                    writer.writerow([table, did, did_value])
                bar.next()
                    
    click.echo("Scan complete.")
            
        
@cli.command(name="tables")
@click.pass_context
def list_tables(context):
    """ List dynamodb tables """
    import boto3

    dynamodb = context.obj['dynamodb']
    tables = list(dynamodb.tables.all())
    for table in tables:
        print(table.name)


@cli.command(name="indexes")
@click.pass_context
def list_indexes(context):
    """ List elasticsearch indexesß """
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        client = context.obj['elastic']

        indices = client.cat.indices(h='index', s='index').split()
        for index in indices:
            print(index)


@cli.command()
@click.option("-eid", required=True, help="ElasticSearch document id field name")
@click.option("-did", required=True, help="DynamoDB document id field name")
@click.option("-t", "--table", required=True, help="DynamoDB table")
@click.option("-i", "--index", required=True, help="ElasticSearch index")
@click.option("-f", "--file", required=False, help="Dynamo ID file created from scan")
@click.option("-o", "--out", required=False, default="kompare.out", help="CSV output filename")
@click.pass_context
def dyn2es(context, eid, did, table, index, file, out):
    """Scan dynamodb and find matches in elasticsearch"""
    import boto3
    from prettytable import PrettyTable
    from progress.bar import Bar
    import warnings
    import csv

    x = PrettyTable()

    names = ["Dynamo Table", "Elastic Index", "ES Field", "DynamoDB Field",  "Misses", "Total"]

    x.field_names = names

    if file:
        with open(file, 'r') as idfile:
            idcsv = csv.reader(idfile)
            _total = sum(1 for row in idcsv)
        
        dynamo_misses = 0
        with open(out, 'w') as csvfile:
            writer = csv.writer(csvfile)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                with open(file, 'r') as idfile:
                    idcsv = csv.reader(idfile)
                    
                    bar = Bar('Scanning', max=_total)
                    idcsv = csv.reader(idfile)
                    logging.debug("file mode: %s",_total)
                    for row in idcsv:
                        logging.debug("ROW %s",row)
                        _did = row[2]
                        if not check_elastic(context.obj['elastic'], index, eid, did):
                            dynamo_misses += 1
                            writer.writerow([table, index, eid, did, _did])
                        logging.debug("Checking elastic")
                        bar.next()

    if False:
        pass       
    else:
        with open(out, 'w') as csvfile:

            writer = csv.writer(csvfile)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                dynamo_misses = 0

                dynamodb = context.obj['dynamodb']
                _table = dynamodb.Table(table)
                _total = _table.item_count
                bar = Bar('Scanning', max=_total)
                _docs = []
                
                for doc in scan(_table):
                    did_value = doc[did]
                    if not check_elastic(context.obj['elastic'], index, eid, did_value):
                        dynamo_misses += 1
                        writer.writerow([table, index, eid, did, did_value])
                        if sync and len(_docs) == 100:
                            logging.debug("Syncing %s documents to elasticsearch index[%s]", str(len(_docs)), index)
                            context.obj['elastic'].bulk((context.obj['elastic'].index_op(doc, id=doc[did]) for doc in _docs),
                                    index=index,
                                    doc_type='_doc')
                            _docs.clear()

                    bar.next()

                if sync and len(_docs):
                    logging.debug("Syncing %s documents to elasticsearch index[%s]", str(len(_docs)), index)
                    context.obj['elastic'].bulk((context.obj['elastic'].index_op(doc, id=doc[did]) for doc in _docs),
                                        index=index,
                                        doc_type='_doc')
                x.add_row([table, index, eid, did, dynamo_misses, _total])
                print()
                print(x)


@cli.command()
@click.option("-eid", required=True, help="ElasticSearch document id field name")
@click.option("-did", required=True, help="DynamoDB document id field name")
@click.option("-t", "--table", required=True, help="DynamoDB table")
@click.option("-i", "--index", required=True, help="ElasticSearch index")
@click.option("-c", "--csv", required=False, is_flag=True, default=False, help="Output all differences in CSV file")
@click.option("-o", "--out", required=False, default="kompare.out", help="CSV output filename")
@click.pass_context
def es2dyn(context, eid, did, table, index, csv, out):
    """Scan elasticsearch and find matches in dynamodb"""
    from prettytable import PrettyTable
    from progress.bar import Bar
    import warnings
    import csv

    x = PrettyTable()

    names = ["Dynamo Table", "ES Field", "DynamoDB Field", "Elastic Index", "Misses", "Total"]

    x.field_names = names

    with open(out, 'w') as csvfile:

        writer = csv.writer(csvfile)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            client = context.obj['elastic']
            client.indices.refresh(index)
            total_docs = client.cat.count(index, params={"format": "json"})
            _total = int(total_docs[0]['count'])
            bar = Bar('Scanning', max=_total)

            search = Search(using=client, index=index)
            dynamo_misses = 0

            for hit in search.scan():
                if eid in hit:
                    eid_value = hit[eid]
                    if not check_dynamo(context.obj["dynamodb"], table, did, eid_value):
                        dynamo_misses += 1

                        writer.writerow([table, did, eid_value])
                else:
                    logger.error(f"No field {eid} in {hit}")

                bar.next()

            x.add_row([table, index, eid, did, dynamo_misses, _total])
            print()
            print(x)
