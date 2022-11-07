import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

import AWS from "aws-sdk";
import { PassThrough } from "stream";
import { Upload } from "@aws-sdk/lib-storage";
import concat from "concat-stream";
import { finished } from "stream/promises";

// Define s3-upload-stream with S3 credentials.
const s3Stream = require("s3-upload-stream")(
  new AWS.S3({ region: "eu-central-1" })
);

export async function getS3Data<T>({
  bucket,
  key,
  region = "eu-central-1",
  parse = true,
}: {
  bucket: string;
  key: string;
  region?: string;
  parse?: boolean;
}) {
  try {
    const data: Buffer = await getS3Buffer({ bucket, key, region });

    if (parse) return JSON.parse(data.toString()) as T;
    else return data;
  } catch (err) {
    throw err;
  }
}

export async function getS3JSONObject<T>({
  bucket,
  key,
  region = "eu-central-1",
}: {
  bucket: string;
  key: string;
  region?: string;
}) {
  try {
    const data: Buffer = await getS3Buffer({ bucket, key, region });

    return JSON.parse(data.toString()) as T;
  } catch (err) {
    throw err;
  }
}

export async function getS3Stream({
  bucket,
  key,
  region = "eu-central-1",
}: {
  bucket: string;
  key: string;
  region?: string;
}) {
  try {
    // Create an Amazon S3 service client object.
    const s3Client = new S3Client({ region });

    const { Body } = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: key,
      })
    );

    return Body as NodeJS.ReadableStream;
  } catch (err) {
    throw err;
  }
}

export async function getS3Buffer({
  bucket,
  key,
  region = "eu-central-1",
}: {
  bucket: string;
  key: string;
  region?: string;
}) {
  try {
    let data: Buffer = Buffer.from("");
    const readStream = await getS3Stream({ bucket, key, region });
    const concatStream = concat((d) => {
      data = d;
    });

    readStream.pipe(concatStream);
    await finished(concatStream);

    return data;
  } catch (err) {
    throw err;
  }
}

export async function saveToS3({
  bucket,
  key,
  body,
  region = "eu-central-1",
}: {
  bucket: string;
  key: string;
  body: string | Record<string, any> | any[];
  region?: string;
}) {
  try {
    const putObjParams = {
      Bucket: bucket,
      Key: key,
      Body: typeof body === "string" ? body : JSON.stringify(body),
    };

    // Create an Amazon S3 service client object.
    const s3Client = new S3Client({ region });

    //   update our bucket
    await s3Client.send(new PutObjectCommand(putObjParams));
  } catch (err) {
    throw err;
  }
}

export async function uploadFileToS3({
  bucket,
  key,
  readableStream,
  region = "eu-central-1",
}: {
  bucket: string;
  key: string;
  readableStream: NodeJS.ReadableStream;
  region?: string;
  progress: Boolean;
}) {
  const passThroughStream = new PassThrough();
  const target = { Bucket: bucket, Key: key, Body: passThroughStream };
  let res;

  try {
    const parallelUploads3 = new Upload({
      client: new S3Client({ region }),
      leavePartsOnError: false, // optional manually handle dropped parts
      params: target,
    });

    readableStream.pipe(passThroughStream);
    res = await parallelUploads3.done();
  } catch (e) {
    throw e;
  }

  return res;
}

// Handle uploading file to Amazon S3.
// Uses the multipart file upload API.
async function uploadS3({
  bucket,
  key,
  readableStream,
  region = "eu-central-1",
}: {
  bucket: string;
  key: string;
  readableStream: NodeJS.ReadableStream;
  region?: string;
  progress: Boolean;
}) {
  return new Promise((resolve, reject) => {
    const upload = s3Stream.upload({
      Bucket: bucket,
      Key: key,
    });

    // Handle errors.
    upload.on("error", function (err: Error) {
      reject(err);
    });

    // // Handle progress.
    // // upload.on('part', function (details) {
    // //   console.log(inspect(details));
    // // });

    // Handle upload completion.
    upload.on("uploaded", function (details: any) {
      resolve(details);
    });

    // Pipe the Readable stream to the s3-upload-stream module.
    readableStream.pipe(upload);
  });
}
