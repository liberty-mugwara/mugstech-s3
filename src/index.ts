import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

import concat from "concat-stream";
import { pipeline } from "stream/promises";

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
    // Create an Amazon S3 service client object.
    const s3Client = new S3Client({ region });
    let data: Buffer = Buffer.from("");

    const { Body } = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: key,
      })
    );

    await pipeline<NodeJS.ReadableStream, NodeJS.WritableStream>(
      Body as NodeJS.ReadableStream,
      concat((d) => {
        data = d;
      })
    );

    if (parse) return JSON.parse(data.toString()) as T;
    else return data;
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
