const AWS = require('aws-sdk');

const s3 = new AWS.S3({
  region: 'us-east-1',
  signatureVersion: 'v4',
});

module.exports.handler = async event => {
  const body = JSON.parse(event.body);
  const { path, bucket, expires, acl, contentType } = body;

  const params = {
    Bucket: bucket,
    Key: path,
    Expires: expires || 3600,
    ACL: acl || 'public-read',
    ContentType: contentType,
  };

  const url = await s3.getSignedUrlPromise('putObject', params);

  return {
    statusCode: 200,
    body: JSON.stringify({
      url,
    }),
  };
};
