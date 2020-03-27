const AWS = require('aws-sdk');

const textract = new AWS.Textract({
  region: 'us-east-1',
});

module.exports.handler = async event => {
  // Parse Bucket and Name from S3 Notification Event
  let Bucket, Name;
  try {
    ({
      bucket: { name: Bucket },
      object: { key: Name },
    } = JSON.parse(
      JSON.parse(event.Records[0].body).Message,
    ).Records[0].s3);
  } catch {
    console.error(
      'Error parsing S3 event',
      JSON.stringify(event, null, 2),
    );
  }

  // TODO: Role and SNS are hardcoded
  const params = {
    DocumentLocation: {
      S3Object: {
        Bucket,
        Name,
      },
    },
    NotificationChannel: {
      RoleArn:
        'arn:aws:iam::527448467163:role/backend-dev-TextractRole-JC73ZZNLZ2SA',
      SNSTopicArn: 'arn:aws:sns:us-east-1:527448467163:textractTopic',
    },
  };
  const response = await textract
    .startDocumentTextDetection(params)
    .promise();

  console.log(response);

  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        response,
      },
      null,
      2,
    ),
  };
};
