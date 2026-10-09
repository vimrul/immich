import {
  CopyObjectCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
  paginateListObjectsV2,
} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { join, relative, resolve, sep } from 'node:path';
import { Readable } from 'node:stream';
import { StorageFolder } from 'src/enum.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';

export type S3Object = { Key?: string; Size?: number; LastModified?: Date };

@Injectable()
export class S3StorageRepository implements OnModuleDestroy {
  private readonly client?: S3Client;
  private readonly bucket?: string;
  private readonly root: string;
  private readonly prefix: string;

  constructor(configRepository: ConfigRepository) {
    const { storage } = configRepository.getEnv();
    const s3 = storage.s3 ?? { region: 'us-east-1', forcePathStyle: false };
    this.root = resolve(storage.mediaLocation ?? '/data');
    this.prefix = (s3.prefix ?? '').replaceAll(/^\/+|\/+$/g, '');

    if (storage.backend !== 's3') {
      return;
    }

    if (!s3.bucket) {
      throw new Error('IMMICH_S3_BUCKET is required when IMMICH_STORAGE_BACKEND is s3');
    }

    this.bucket = s3.bucket;
    this.client = new S3Client({
      region: s3.region,
      endpoint: s3.endpoint,
      forcePathStyle: s3.forcePathStyle,
    });
  }

  get enabled() {
    return !!this.client;
  }

  managesPath(filepath: string) {
    if (!this.enabled) {
      return false;
    }

    const resolved = resolve(filepath);
    return resolved === this.root || resolved.startsWith(`${this.root}${sep}`);
  }

  isHlsPath(filepath: string) {
    if (!this.managesPath(filepath)) {
      return false;
    }

    const parts = relative(this.root, resolve(filepath)).split(sep);
    return parts.length >= 3 && parts[0] === StorageFolder.EncodedVideo && !parts[2].includes('.');
  }

  localPath(filepath: string) {
    if (!this.isHlsPath(filepath)) {
      return filepath;
    }

    const parts = relative(this.root, resolve(filepath)).split(sep);
    return join(this.root, '.immich-cache', 'hls', ...parts.slice(1));
  }

  key(filepath: string) {
    if (!this.managesPath(filepath)) {
      throw new Error(`Path is outside the configured S3 storage root: ${filepath}`);
    }

    const path = relative(this.root, resolve(filepath)).split(sep).join('/');
    return [this.prefix, path].filter(Boolean).join('/');
  }

  async getObject(key: string, range?: string) {
    return this.client!.send(new GetObjectCommand({ Bucket: this.bucket, Key: key, Range: range }));
  }

  async headObject(key: string) {
    return this.client!.send(new HeadObjectCommand({ Bucket: this.bucket, Key: key }));
  }

  path(key: string) {
    let relativeKey = key;
    if (this.prefix) {
      const prefix = `${this.prefix}/`;
      if (!key.startsWith(prefix)) {
        throw new Error(`S3 object key is outside the configured prefix: ${key}`);
      }
      relativeKey = key.slice(prefix.length);
    }

    const segments = relativeKey.split('/');
    if (segments.some((segment) => !segment || segment === '.' || segment === '..')) {
      throw new Error(`S3 object key is not a valid storage path: ${key}`);
    }

    const filepath = join(this.root, ...segments);
    const relativePath = relative(this.root, filepath);
    if (relativePath === '..' || relativePath.startsWith(`..${sep}`)) {
      throw new Error(`S3 object key resolves outside the configured storage root: ${key}`);
    }
    return filepath;
  }

  async putObject(key: string, body: Buffer | Uint8Array, metadata?: Record<string, string>) {
    await this.client!.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: body,
        Metadata: metadata,
      }),
    );
  }

  async upload(key: string, body: Readable, metadata?: Record<string, string>) {
    await new Upload({
      client: this.client!,
      params: { Bucket: this.bucket, Key: key, Body: body, Metadata: metadata },
      leavePartsOnError: false,
    }).done();
  }

  async copy(sourceKey: string, targetKey: string) {
    await this.client!.send(
      new CopyObjectCommand({
        Bucket: this.bucket,
        Key: targetKey,
        CopySource: `${this.bucket}/${sourceKey
          .split('/')
          .map((segment) => encodeURIComponent(segment))
          .join('/')}`,
      }),
    );
  }

  async setTimes(key: string, atime: Date, mtime: Date) {
    const source = await this.headObject(key);
    const metadata = {
      ...source.Metadata,
      'immich-atime': atime.toISOString(),
      'immich-mtime': mtime.toISOString(),
    };
    if ((source.ContentLength ?? 0) > 5_000_000_000) {
      const { Body } = await this.getObject(key);
      if (!Body) {
        throw new Error(`S3 object ${key} has no body`);
      }
      await this.upload(key, Body as Readable, metadata);
      return;
    }

    await this.client!.send(
      new CopyObjectCommand({
        Bucket: this.bucket,
        Key: key,
        CopySource: `${this.bucket}/${key
          .split('/')
          .map((segment) => encodeURIComponent(segment))
          .join('/')}`,
        MetadataDirective: 'REPLACE',
        Metadata: metadata,
        ContentType: source.ContentType,
        CacheControl: source.CacheControl,
        ContentDisposition: source.ContentDisposition,
        ContentEncoding: source.ContentEncoding,
        ContentLanguage: source.ContentLanguage,
        Expires: source.Expires,
      }),
    );
  }

  async delete(key: string) {
    await this.client!.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: key }));
  }

  async *list(prefix: string, delimiter?: string): AsyncGenerator<S3Object[]> {
    const pages = paginateListObjectsV2(
      { client: this.client!, pageSize: 1000 },
      {
        Bucket: this.bucket,
        Prefix: prefix,
        Delimiter: delimiter,
      },
    );

    for await (const page of pages) {
      yield page.Contents ?? [];
    }
  }

  async *listDirectory(prefix: string) {
    const pages = paginateListObjectsV2(
      { client: this.client!, pageSize: 1000 },
      { Bucket: this.bucket, Prefix: prefix, Delimiter: '/' },
    );
    for await (const page of pages) {
      yield { contents: page.Contents ?? [], prefixes: page.CommonPrefixes ?? [] };
    }
  }

  async listPage(prefix: string, delimiter?: string, maxKeys?: number) {
    const page = await this.client!.send(
      new ListObjectsV2Command({ Bucket: this.bucket, Prefix: prefix, Delimiter: delimiter, MaxKeys: maxKeys }),
    );
    return page;
  }

  async removePrefix(prefix: string) {
    for await (const objects of this.list(prefix)) {
      if (objects.length === 0) {
        continue;
      }

      const response = await this.client!.send(
        new DeleteObjectsCommand({
          Bucket: this.bucket,
          Delete: {
            Objects: objects.flatMap(({ Key }) => (Key ? [{ Key }] : [])),
            Quiet: true,
          },
        }),
      );
      if (response.Errors?.length) {
        throw new Error(
          `Failed to delete S3 object(s): ${response.Errors.map(({ Key, Code }) => `${Key}: ${Code}`).join(', ')}`,
        );
      }
    }
  }

  destroy() {
    this.client?.destroy();
  }

  onModuleDestroy() {
    return this.destroy();
  }
}
