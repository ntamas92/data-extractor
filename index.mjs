import * as AWS from "@aws-sdk/client-s3";
import pg from "pg";
const { Client } = pg;
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from "@aws-sdk/client-secrets-manager";

const getDatabaseSecret = async () => {
  const secret_name = "hw-uda-curatedzone-creds2";

  const client = new SecretsManagerClient({
    region: "us-west-2",
  });

  let response;

  try {
    response = await client.send(
      new GetSecretValueCommand({
        SecretId: secret_name,
        VersionStage: "AWSCURRENT", // VersionStage defaults to AWSCURRENT if unspecified
      })
    );
  } catch (error) {
    // For a list of exceptions thrown, see
    // https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    throw error;
  }

  const { username, password } = JSON.parse(response.SecretString);

  return { username, password };
};

const getRequestParams = (bucketName, objectKey, resourceKey) => {
  console.log(`SELECT * FROM S3Object[*].${resourceKey}[*]`);

  const params = {
    Bucket: bucketName,
    Key: objectKey,
    ExpressionType: "SQL",
    Expression: `SELECT * FROM S3Object[*].${resourceKey}[*]`,
    InputSerialization: {
      JSON: {
        Type: "DOCUMENT",
      },
      CompressionType: "GZIP",
    },
    OutputSerialization: {
      JSON: {
        Type: "DOCUMENT",
      },
    },
  };

  return params;
};

const selectData = async (s3, bucketName, objectKey, resourceKey) => {
  const requestParams = getRequestParams(bucketName, objectKey, resourceKey);

  const data = await s3.selectObjectContent(requestParams);
  // data.Payload is a Readable Stream
  const eventStream = data.Payload;

  const buffer = [];

  for await (const event of eventStream) {
    if (event.Records?.Payload) {
      console.log(event.Records.Payload);
      buffer.push(Array.from(event.Records.Payload));
    }
  }

  const lines = Buffer.from(buffer.flat())
    .toString()
    .split(/\r?\n/)
    .filter((x) => !!x);

  const records = lines.map((x) => JSON.parse(x.trim()));
  console.log("buff", records);

  return records;
};

const getAgentId = (object) => {
  console.log(object);
  const pattern = /agents\/(.*)\/rosterings/;
  const agentId = object.key.match(pattern)[1];
  return agentId;
};

export const handler = async (event) => {
  var s3 = new AWS.S3();
  const { bucket, object } = event.Records[0].s3;

  const agentId = getAgentId(object);
  console.log(agentId);

  const bucketName = bucket.name;
  const objectKey = object.key;

  const databaseCredentials = await getDatabaseSecret();
  const databaseProxyUrl =
    "proxy-1677772908682-hw-uda-curatedzone.proxy-c9wrscxi9ap9.us-west-2.rds.amazonaws.com";

  try {
    const client = new Client({
      user: databaseCredentials.username,
      host: databaseProxyUrl,
      database: "rostering",
      password: databaseCredentials.password,
      port: 5432,
    });

    await client.connect();

    const accountsData = await selectData(
      s3,
      bucketName,
      objectKey,
      "accounts"
    );

    for (const account of accountsData) {
      const { account_id, name, parent_account_id, status, _original } =
        account;
      await client.query(
        "INSERT INTO accounts (account_id, name, parent_account_id, last_modified, status, agent_id, _original) " +
          "VALUES ($1, $2, $3, NOW(), $4, $5, $6) " +
          "ON CONFLICT (account_id, agent_id) DO UPDATE " +
          "SET name = $2, parent_account_id = $3, last_modified = NOW(), status = $4, _original = $6",
        [account_id, name, parent_account_id, status, agentId, _original]
      );
    }

    const usersData = await selectData(s3, bucketName, objectKey, "users");

    for (const userData of usersData) {
      const {
        user_id,
        first_name,
        last_name,
        email,
        login_id,
        status,
        _original,
      } = userData;
      await client.query(
        "INSERT INTO users (user_id, first_name, last_name, email, login_id, status, _original, last_modified, agent_id) " +
          "VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8) " +
          "ON CONFLICT (user_id, agent_id) DO UPDATE " +
          "SET first_name=$2, last_name=$3, email=$4, login_id=$5, status=$6, _original=$7, last_modified=NOW()",
        [
          user_id,
          first_name,
          last_name,
          email,
          login_id,
          status,
          _original,
          agentId,
        ]
      );
    }

    const coursesData = await selectData(s3, bucketName, objectKey, "courses");

    for (const course of coursesData) {
      const {
        course_id,
        account_id,
        short_name,
        long_name,
        status,
        _original,
      } = course;
      await client.query(
        "INSERT INTO courses (course_id, account_id, short_name, long_name, status, _original, last_modified, agent_id) " +
          "VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7) " +
          "ON CONFLICT (course_id, agent_id) DO UPDATE " +
          "SET account_id=$2, short_name=$3, long_name=$4, status=$5, _original=$6, last_modified=NOW()",
        [
          course_id,
          account_id,
          short_name,
          long_name,
          status,
          _original,
          agentId,
        ]
      );
    }

    const sectionsData = await selectData(
      s3,
      bucketName,
      objectKey,
      "sections"
    );

    for (const section of sectionsData) {
      const { section_id, course_id, name, status, _original } = section;
      await client.query(
        "INSERT INTO sections (section_id, course_id, name, status, _original, last_modified, agent_id) " +
          "VALUES ($1, $2, $3, $4, $5, NOW(), $6) " +
          "ON CONFLICT (section_id, agent_id) DO UPDATE " +
          "SET course_id=$2, name=$3, status=$4, _original=$5, last_modified=NOW()",
        [section_id, course_id, name, status, _original, agentId]
      );
    }

    const termsData = await selectData(s3, bucketName, objectKey, "terms");

    for (const term of termsData) {
      const { term_id, name, start_date, end_date, status, _original } = term;

      if (!term_id) continue;

      await client.query(
        "INSERT INTO terms (term_id, name, start_date, end_date, status, _original, last_modified, agent_id) " +
          "VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7) " +
          "ON CONFLICT (term_id, agent_id) DO UPDATE " +
          "SET name=$2, start_date=$3, end_date=$4, status=$5, _original=$6, last_modified=NOW()",
        [term_id, name, start_date, end_date, status, _original, agentId]
      );
    }

    const enrollmentsData = await selectData(
      s3,
      bucketName,
      objectKey,
      "enrollments"
    );

    for (const enrollment of enrollmentsData) {
      const { user_id, section_id, role, status, _original } = enrollment;

      if (!user_id || !section_id) continue;

      await client.query(
        "INSERT INTO enrollments (user_id, section_id, role, status, _original, last_modified, agent_id) " +
          "VALUES ($1, $2, $3, $4, $5, NOW(), $6) " +
          "ON CONFLICT (user_id, section_id, agent_id) DO UPDATE " +
          "SET section_id=$2, role=$3, status=$4, _original=$5, last_modified=NOW()",
        [user_id, section_id, role, status, _original, agentId]
      );
    }

    return {
      statusCode: 200,
      body: JSON.stringify("Hello from Lambda!"),
    };
  } catch (err) {
    console.log(err);
  }
};
