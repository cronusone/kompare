import logging
import sys
import configparser
import click
from termcolor import colored
from elasticsearch_dsl import Search
import elasticsearch
from elasticsearch import helpers

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


@click.group(invoke_without_command=True)
@click.option("--debug", is_flag=True, default=False, help="Debug switch")
@click.option("-c", "--configfile", required=False, default="kompare.ini", help="Path to kompare.ini file")
@click.pass_context
def cli(context, debug, configfile):
    import boto3

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    config.read(configfile)
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


def check_elastic(elastic, index, eid, eid_value, dump):
    
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
    logging.debug("QUERY %s",query)
    logging.debug("check_elastic: %s", results)
    logging.debug("FOUND %s hits", results['hits']['total']['value'])
    result = results['hits']['total']['value'] >= 1
    if result == 0:
        return False
    
    for r in results['hits']['hits']:
        logging.info(r['_source'][eid])
        if dump:
            print(r['_source'])
            
    result = False
    for doc in results['hits']['hits']:
        if doc['_source'][eid] == eid_value:
            result = True
            break
        
    CACHE[eid_value] = result
        
    logging.debug("check_elastic: returning %s", result)
    return result


def check_dynamo(dynamodb, table, did, eid_value, dump):
    from boto3.dynamodb.conditions import Key

    table = dynamodb.Table(table)
    response = table.query(
        KeyConditionExpression=Key(did).eq(eid_value)
    )

    if dump:
        print(response['Items'])
    return response['Count'] > 0


def scan(table, lastkey, **kwargs):
    import json
    
    if lastkey:
        print("Resuming from Last Key ", lastkey)
        response = table.scan(ExclusiveStartKey=lastkey, **kwargs)
    else:
        response = table.scan(**kwargs)
    
    while response.get('LastEvaluatedKey'):
        
        with open('lasteval.key', 'w') as lastkey:
            lastkey.write(json.dumps(response['LastEvaluatedKey']))
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
            yield from response['Items']
    
@cli.command(name="check")
@click.option("-f", "--field", required=False, help="Document id field name")
@click.option("-v", "--value", required=False, help="ID of document")
@click.option("-t", "--table", required=False, help="DynamoDB table")
@click.option("-i", "--index", required=False, default="customersAlias", help="ElasticSearch index")
@click.option("-o", "--out", required=True, default="kompare-check.csv", help="Name of output file")
@click.option("-in", "--infile", required=False, default=None, help="Scan output file containing dynamoids")
@click.option("-s", "--source", type=click.Choice(['elastic', 'dynamo'], case_sensitive=False), help="Database source")
@click.option("-sy", "--sync", required=False, is_flag=True, default=False, help="Sync missing documents to elasticsearch")
@click.option("-d", "--dump", required=False, is_flag=True, default=False, help="Dump the JSON for the record(s)")
@click.pass_context
def check(context, field, value, table, index, out, infile, source, sync, dump):
    """Check if a document exists"""
    import csv
    import warnings
    from progress.bar import Bar
    from boto3.dynamodb.conditions import Key

    with warnings.catch_warnings():
        warnings.simplefilter("ignore") 
        dynamodb = context.obj['dynamodb']
        
        if not infile:
            if source == 'elastic':
                print(check_elastic(context.obj['elastic'], index, field, value, dump))
                if sync:
                    sync_docs = []
                    _table = dynamodb.Table(table)
                    response = _table.query(KeyConditionExpression=Key(field).eq(value))
                    s_doc = {
                        "_index": index,
                        "_type" : "_doc",
                        "_id"   : value,
                        "_source": response['Items'][0]
                    }
                    if dump:
                        print(s_doc)
                    sync_docs.append(s_doc)
                    helpers.bulk(context.obj['elastic'], sync_docs)
                    
            if source == 'dynamo':
                print(check_dynamo(context.obj["dynamodb"], table,field,value, dump))
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
                            _docs = []
                            for row in idcsv:
                                r_table = row[0]
                                r_index = row[1]
                                r_eid = row[2]
                                r_did = row[3]
                                r_value = row[4]
                                _table = dynamodb.Table(r_table)
                                logging.debug("ROW %s",row)
                                logging.debug("Checking elastic")
                                if not check_elastic(context.obj['elastic'], r_index, r_eid, r_value, False):
                                    dynamo_misses += 1
                                    writer.writerow(row)
                                    _docs.append(row)
                                    if sync and len(_docs) == 100:
                                        sync_docs = []
                                        for sync_doc in _docs:
                                            try:
                                                _r_table = sync_doc[0]
                                                _r_index = sync_doc[1]
                                                _r_eid = sync_doc[2]
                                                _r_did = sync_doc[3]
                                                _r_value = sync_doc[4]
                                                logging.debug("Fetching document %s:%s from table %s", _r_did, _r_value, _r_table)
                                                response = _table.query(
                                                    KeyConditionExpression=Key(_r_did).eq(_r_value)
                                                )
                                                s_doc = {
                                                    "_index": _r_index,
                                                    "_type" : "_doc",
                                                    "_id"   : _r_value,
                                                    "_source": response['Items'][0]
                                                }
                                                sync_docs.append(s_doc)
                                            
                                            except Exception as ex:
                                                logging.error(ex)
                                                
                                        logging.debug("Syncing docs %s",sync_docs)
                                        helpers.bulk(context.obj['elastic'], sync_docs)
                                        _docs.clear()
                                    
                                    if len(_docs):
                                        sync_docs = []
                                        for sync_doc in _docs:
                                            try:
                                                _r_table = sync_doc[0]
                                                _r_index = sync_doc[1]
                                                _r_eid = sync_doc[2]
                                                _r_did = sync_doc[3]
                                                _r_value = sync_doc[4]
                                                logging.debug("Fetching document %s:%s from table %s", _r_did, _r_value, _r_table)
                                                response = _table.query(
                                                    KeyConditionExpression=Key(_r_did).eq(_r_value)
                                                )
                                                s_doc = {
                                                    "_index": _r_index,
                                                    "_type" : "_doc",
                                                    "_id"   : _r_value,
                                                    "_source": response['Items'][0]
                                                }
                                                sync_docs.append(s_doc)
                                            
                                            except Exception as ex:
                                                logging.error(ex)
                                                
                                        logging.debug("Syncing %s documents to elasticsearch index[%s]", str(len(sync_docs)), _r_index)
                                        
                                        helpers.bulk(context.obj['elastic'], sync_docs)
                                bar.next()
            else:
                pass
    
def do_trim(table, docid, idvalue):
    import dateutil.parser
    import boto3
    from boto3.dynamodb.conditions import Key

    logging.debug("deduping %s %s", docid, idvalue)
    response = table.query(
        KeyConditionExpression=Key(docid).eq(idvalue)
    )

    items = []
    for doc in response['Items']:

        try:
            doc['_datetime'] = dateutil.parser.isoparse(doc['createTimeStamp'])
            items += [doc]
        except Exception as ex: 
            print("ERROR", doc, doc['createTimeStamp'], doc[docid])
            print(ex)
            continue
            
    sorted_docs = sorted(items, key = lambda x:x['_datetime'])
    sorted_docs.reverse()
    latest = sorted_docs[0]
    to_delete = sorted_docs[1:]
    logging.debug("%s %s", len(sorted_docs), latest['createTimeStamp'])
    logging.debug("%s", [doc['createTimeStamp'] for doc in sorted_docs])
    
    for doc in to_delete:
        fields = {} #{'createTimeStamp':doc['createTimeStamp']}
        fields[docid] = idvalue
        fields['sortKey'] = doc['createTimeStamp']
        response = table.delete_item(Key=fields)
        print("Deleted duplicate doc",idvalue, doc['createTimeStamp'])
    print("Keeping latest version",latest[docid], latest['createTimeStamp'])
    
@cli.command(name="scan")
@click.option("-did", required=False, help="DynamoDB document id field name")
@click.option("-t", "--table", required=True, help="DynamoDB table")
@click.option("-o", "--out", required=False, help="Name of output file")
@click.option("-tr", "--trim", required=False, is_flag=True, default=False, help="Remove older duplicate doc id's")
@click.option("-s","--size",required=False, is_flag=True, default=False, help="Report only size of table")
@click.pass_context
def scan_ids(context, did, table, out, trim, size):
    """ Scan dynamo table and store id's in a file """
    import json
    import os
    import warnings
    import csv
    from progress.bar import Bar

    dynamodb = context.obj['dynamodb']
    
    _table = dynamodb.Table(table)
    _total = _table.item_count
    if size:
        print(_total)
        return
    
    bar = Bar('Scanning', max=_total)
    
    with open(out, 'w') as csvfile:

        writer = csv.writer(csvfile)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            seen = []
            
            _lastkey = None
            if os.path.exists('lasteval.key'):
                with open('lasteval.key', 'r') as lastkey:
                    _lastkey = json.loads(lastkey.read())
                    logging.debug("READ Last key %s", _lastkey)
            
            for doc in scan(_table, _lastkey, ConsistentRead=False):
                did_value = doc[did]
                    
                if did_value not in seen:
                    if trim:
                        do_trim(_table, did, did_value)
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
                        if not check_elastic(context.obj['elastic'], index, eid, _did, False):
                            dynamo_misses += 1
                            writer.writerow([table, index, eid, did, _did])
                        logging.debug("Checking elastic")
                        bar.next()

                    x.add_row([table, index, eid, did, dynamo_misses, _total])
                    print()
                    print(x)
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
                
                for doc in scan(_table):
                    did_value = doc[did]
                    if not check_elastic(context.obj['elastic'], index, eid, did_value, False):
                        dynamo_misses += 1
                        writer.writerow([table, index, eid, did, did_value])

                    bar.next()

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
