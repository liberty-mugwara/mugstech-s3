import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

import { PassThrough } from "stream";
import { Upload } from "@aws-sdk/lib-storage";
import concat from "concat-stream";
import { finished, pipeline } from "stream/promises";
import { createGunzip } from "node:zlib";

export async function getS3Data<T>({
  bucket,
  key,
  region = "eu-central-1",
  parse = true,
  gziped = false,
}: {
  bucket: string;
  key: string;
  region?: string;
  parse?: boolean;
  gziped?: boolean;
}) {
  try {
    const data: Buffer = await getS3Buffer({ bucket, key, region, gziped });

    if (parse) return JSON.parse(data.toString()) as T;
    else return data;
  } catch (err) {
    throw err;
  }
}

export async function getS3JSONObject<T>({
  bucket,
  key,
  gziped = false,
  region = "eu-central-1",
}: {
  bucket: string;
  key: string;
  gziped?: boolean;
  region?: string;
}) {
  try {
    const data: Buffer = await getS3Buffer({ bucket, key, region, gziped });

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
  gziped = false,
  region = "eu-central-1",
}: {
  bucket: string;
  key: string;
  gziped?: boolean;
  region?: string;
}) {
  try {
    let data: Buffer = Buffer.from("");
    const readStream = await getS3Stream({ bucket, key, region });
    const concatStream = concat((d) => {
      data = d;
    });

    if (!gziped) {
      readStream.pipe(concatStream);
      await finished(concatStream);
      return data;
    } else {
      const gunzip = createGunzip();
      await pipeline(readStream, gunzip, concatStream);
      return data;
    }
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
  progress?: Boolean;
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
