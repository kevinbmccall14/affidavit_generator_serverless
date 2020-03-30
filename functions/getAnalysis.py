import json
import uuid
import boto3
from helpers.textractParser import Document
from decimal import Decimal

textract = boto3.client('textract')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Case-eag5diu73rehxez47mn7fvcc7y-dev')


def removeEmptyString(dic):
    for e in dic:
        if isinstance(dic[e], dict):
            dic[e] = removeEmptyString(dic[e])
        if (isinstance(dic[e], str) and dic[e] == ""):
            dic[e] = None
        if isinstance(dic[e], list):
            for entry in dic[e]:
                removeEmptyString(entry)
    return dic


def processDocument(doc):
    # item to PUT in DynamoDB
    item = {
        'id': str(uuid.uuid4()),
        'pages': []
    }
    for page in doc.pages:
        newPage = {
            'lines': [],
            'tables': [],
            'fields': [],
        }
        for line in page.lines:
            newLine = {
                'text': line.text,
                'confidence': Decimal(line.confidence),
                'words': [],
            }
            for word in line.words:
                newLine['words'].append({
                    'text': word.text,
                    'confidence': Decimal(word.confidence),
                })
            newPage['lines'].append(newLine)
        for table in page.tables:
            newTable = {'cells': []}
            for r, row in enumerate(table.rows):
                for c, cell in enumerate(row.cells):
                    newTable['cells'].append({
                        'row': r,
                        'column': c,
                        'text': cell.text,
                        'confidence': Decimal(cell.confidence)
                    })
            newPage['tables'].append(newTable)
        for field in page.form.fields:
            newField = {}
            k = ""
            v = ""
            if(field.key):
                k = field.key.text
            if(field.value):
                v = field.value.text
            newPage['fields'].append({
                'key': k,
                'value': v
            })
        item['pages'].append(newPage)
    return removeEmptyString(item)

    # Example Searching for field could be used to find values
    # key = "CLAIM NUMBER"
    # print("\nSearch field by key ({}):\n====================".format(key))
    # fields = page.form.searchFieldsByKey(key)
    # for field in fields:
    #     print("Field: Key: {}, Value: {}".format(field.key, field.value))


def handler(event, context):
    # Parse JobID from TextractQueue
    for record in event['Records']:
        message = json.loads(record["body"])["Message"]
        parsed_message = json.loads(message)

    # 500 level error will keep event on TextractQueue
    # TextractQueue can configure max retry attempts and event age
    if parsed_message["Status"] != 'SUCCEEDED':
        return {
            'statusCode': 500,
            'body': json.dumps(parsed_message)
        }

    print('Found successful job:', parsed_message["JobId"])

    # Get successfull analysis JSON
    analysis = textract.get_document_analysis(
        JobId=parsed_message["JobId"]
    )

    # parse json into Document object with textractParser
    doc = Document(analysis)
    # process parsed document into dictionary item
    item = processDocument(doc)
    # save item into DynamoDB
    response = table.put_item(Item=item)

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
