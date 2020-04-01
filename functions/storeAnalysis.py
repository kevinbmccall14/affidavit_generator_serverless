import json
import boto3
import os

textract = boto3.client('textract')
s3 = boto3.resource('s3')


def getFullDocumentAnalysis(JobId):
    paginationToken = None
    finished = False
    results = None
    calls = 0
    while finished == False:
        calls += 1
        response = None
        if paginationToken == None:
            response = textract.get_document_analysis(JobId=JobId)
            if results == None:
                results = response
        else:
            response = textract.get_document_analysis(
                JobId=JobId, NextToken=paginationToken)

        # build up block information
        blocks = response['Blocks']
        results['Blocks'] += blocks
        print('## ANALYZING DOCUMENT CALLS:', calls)

        if 'NextToken' in response:
            paginationToken = response['NextToken']
        else:
            finished = True
    return results


def handler(event, context):
    # Parse JobID from TextractQueue
    print('## EVENT', event)
    for record in event['Records']:
        message = json.loads(record["body"])["Message"]
        parsed_message = json.loads(message)

    print('## PARSED MESSAGE:', parsed_message)
    # 500 level error will keep event on TextractQueue
    # TextractQueue can configure max retry attempts and event age
    if parsed_message["Status"] != 'SUCCEEDED':
        return {
            'statusCode': 500,
            'body': json.dumps(parsed_message)
        }

    print('## FOUND JOB:', parsed_message["JobId"])

    # Get successfull analysis JSON
    analysis = getFullDocumentAnalysis(JobId=parsed_message["JobId"])

    # parse file and bucket names for fileUrl metadata
    fileName = parsed_message["DocumentLocation"]['S3ObjectName']
    bucketName = parsed_message["DocumentLocation"]['S3Bucket']
    fileUrl = 'https://' + bucketName + '.s3.amazonaws.com/' + fileName
    filePath = parsed_message["JobId"] + '.json'
    s3object = s3.Object('backend-dev-analysisbucket-1gchwy45f8j4n', filePath)

    # # TODO: save analysis file url in DynamoDB
    # updateAnalysisResponse = updateFile(item={
    #     'id': fileUrl,
    #     'analysis': filePath,
    # })
    # print('## Update Analysis Response:', updateAnalysisResponse)

    # store json file in S3 using jobId as path
    print('## STORING IN S3:', filePath)
    response = s3object.put(
        Body=(bytes(json.dumps(analysis).encode('UTF-8'))),
        Metadata={'fileUrl': fileUrl}
    )

    print('## RESPONSE')
    print(response)

    return {
        'statusCode': 200,
        'body': response
    }
