const AWS = require('aws-sdk');

const textract = new AWS.Textract({
  region: 'us-east-1',
});

module.exports.handler = async event => {
  // Parse JobID from TextractQueue
  let statusCode = 200;
  let JobId, Status, DocumentLocation;
  try {
    ({ JobId, Status, DocumentLocation } = JSON.parse(
      JSON.parse(event.Records[0].body).Message,
    ));
  } catch {
    // Malformed requests should be monitored and addressed manually
    statusCode = 400;
    console.error(
      'Error parsing JobID from TextractQueue Message',
      JSON.stringify(event, null, 2),
    );
  }

  // 500 level error will keep event on TextractQueue
  // TextractQueue can configure max retry attempts and event age
  if (Status !== 'SUCCEEDED') {
    statusCode = 500;
  }

  // Get Successfull Document Text JSON
  const response = await textract
    .getDocumentTextDetection({ JobId })
    .promise();

  // TODO: put into Dynamo?
  console.log(response);

  return {
    statusCode,
    body: JSON.stringify(
      {
        response,
        JobId,
        Status,
        DocumentLocation,
      },
      null,
      2,
    ),
  };
};
