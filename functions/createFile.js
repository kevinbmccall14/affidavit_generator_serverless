const axios = require('axios');
const { v4: uuidv4 } = require('uuid');

const createFile = `mutation createFile($id: ID!, $url: String!) {
    createFile(input: {
      id: $id
      url: $url
    }) {
      id
      url
    }
  }
`;

const executeMutation = async event => {
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

  // create mutation
  id = uuidv4();
  url = `https://${Bucket}.s3.amazonaws.com/${Name}`;

  const mutation = {
    query: createFile,
    operationName: 'createFile',
    variables: {
      id,
      url,
    },
  };

  let response;
  try {
    response = await axios({
      method: 'POST',
      url:
        'https://g2pjyzahzjbzpnljtd37fe6v3a.appsync-api.us-east-1.amazonaws.com/graphql',
      data: JSON.stringify(mutation),
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': 'da2-edkhexvrirh7tid7cqdnitfhay',
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

exports.handler = async event => {
  const body = event.body ? JSON.parse(event.body) : event;
  const response = await executeMutation(body);
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
