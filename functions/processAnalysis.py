import json
import uuid
import boto3
import requests
from helpers.textractParser import Document

s3 = boto3.resource('s3')
textract = boto3.client('textract')


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


def processDocument(doc, url):
    # item to PUT in DynamoDB
    item = {
        'id': url,
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
                'confidence': line.confidence,
                'words': [],
            }
            for word in line.words:
                newLine['words'].append({
                    'text': word.text,
                    'confidence': word.confidence,
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
                        'confidence': cell.confidence,
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


def updateFile(item):
    query = """mutation updateFile(
      $input: UpdateFileInput!
    ) {
      updateFile(input: $input) {
        id
      }
    }"""

    mutation = {
        'query': query,
        'operationName': 'updateFile',
        'variables': {
            'input': item
        }
    }

    response = requests.post(
        'https://w4jxfmtcabclrjcfkqvipd4m64.appsync-api.us-east-1.amazonaws.com/graphql',
        data=json.dumps(mutation),
        headers={'Content-Type': 'application/json',
                 'x-api-key': 'da2-np3hr5j7cnfc5d6mmwixisl6z4', }
    )

    return response.json()


def handler(event, context):
    # Parse notification from analysis bucket
    print('## EVENT')
    print(event)
    for record in event['Records']:
        message = json.loads(record["body"])["Message"]
        parsed_message = json.loads(message)

    print('## PARSED MESSAGE')
    print(parsed_message)

    bucket_name = parsed_message['Records'][0]["s3"]['bucket']['name']
    file_name = parsed_message['Records'][0]["s3"]['object']['key']

    content_object = s3.Object(bucket_name, file_name)
    file_content = content_object.get()
    file_body = file_content['Body'].read().decode('utf-8')
    file_metadata = file_content['Metadata']

    print('## FOUND ANALYSIS WITH METADATA:', file_metadata)

    # parse json into Document object with textractParser helper
    doc = Document(json.loads(file_body))

    # process parsed document into dictionary item
    fileUrl = file_metadata['fileurl'].replace('%3A', ':')
    print('## PROCESSING JSON FOR:', fileUrl)
    item = processDocument(doc, fileUrl)

    # save processed pages data into DynamoDB
    response = updateFile(item=item)

    print('## RESPONSE')
    print(response)

    return {
        'statusCode': 200,
        'body': response
    }
