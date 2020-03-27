import json
import boto3
from helpers.textractParser import Document

textract = boto3.client('textract')


def processDocument(doc):
    for page in doc.pages:
        print("PAGE\n====================")
        for line in page.lines:
            print("Line: {}--{}".format(line.text, line.confidence))
            for word in line.words:
                print("Word: {}--{}".format(word.text, word.confidence))
        for table in page.tables:
            print("TABLE\n====================")
            for r, row in enumerate(table.rows):
                for c, cell in enumerate(row.cells):
                    print("Table[{}][{}] = {}-{}".format(r,
                                                         c, cell.text, cell.confidence))
        print("Form (key/values)\n====================")
        for field in page.form.fields:
            k = ""
            v = ""
            if(field.key):
                k = field.key.text
            if(field.value):
                v = field.value.text
            print("Field: Key: {}, Value: {}".format(k, v))

        # Search field by key
        key = "CLAIM NUMBER"
        print("\nSearch field by key ({}):\n====================".format(key))
        fields = page.form.searchFieldsByKey(key)
        for field in fields:
            print("Field: Key: {}, Value: {}".format(field.key, field.value))


def handler(event, context):
    # Parse JobID from TextractQueue
    print(event)
    for record in event['Records']:
        message = json.loads(record["body"])["Message"]
        parsed_message = json.loads(message)
    print(parsed_message)

    # 500 level error will keep event on TextractQueue
    # TextractQueue can configure max retry attempts and event age
    if parsed_message["Status"] != 'SUCCEEDED':
        return {
            'statusCode': 500,
            'body': json.dumps(parsed_message)
        }

    # Get Successfull Document Text JSON
    response = textract.get_document_analysis(
        JobId=parsed_message["JobId"]
    )

    doc = Document(response)
    processDocument(doc)

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
