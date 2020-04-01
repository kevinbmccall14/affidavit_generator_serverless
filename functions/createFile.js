const axios = require('axios');

const parseUrl = event => {
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
  return `https://${Bucket}.s3.amazonaws.com/${Name}`;
};

const executeGQL = async ({ query, operationName, url }) => {
  const gql = {
    query,
    operationName,
    variables: {
      id: url,
    },
  };

  console.log(`Executing GQL operation:`, gql);

  let response;
  try {
    response = await axios({
      method: 'POST',
      url:
        'https://w4jxfmtcabclrjcfkqvipd4m64.appsync-api.us-east-1.amazonaws.com/graphql',
      data: JSON.stringify(gql),
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': 'da2-np3hr5j7cnfc5d6mmwixisl6z4',
      },
    });
  } catch (error) {
    console.error(
      `[ERROR] ${error.response.status} - ${JSON.stringify(
        error.response.data,
      )}`,
    );
    throw error;
  }
  return response;
};

const createFile = `mutation createFile($id: ID!) {
    createFile(input: {
      id: $id
    }) {
      id
    }
  }
`;

exports.handler = async event => {
  const body = event.body ? JSON.parse(event.body) : event;
  const url = parseUrl(body);
  const response = await executeGQL({
    query: createFile,
    operationName: 'createFile',
    url,
  });
  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(
      {
        response: response.data,
      },
      null,
      2,
    ),
  };
};
