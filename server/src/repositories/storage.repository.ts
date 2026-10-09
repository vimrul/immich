import { Injectable, Optional } from '@nestjs/common';
import archiver from 'archiver';
import { ChokidarOptions, watch as chokidarWatch } from 'chokidar';
import fastGlob from 'fast-glob';
import {
  Dirent,
  ReadOptionsWithBuffer,
  Stats,
  constants,
  createReadStream,
  createWriteStream,
  existsSync,
  mkdirSync,
  watch,
} from 'node:fs';
import fs from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { PassThrough, Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createGunzip, createGzip } from 'node:zlib';
import picomatch from 'picomatch';
import { CrawlOptionsDto, WalkOptionsDto } from 'src/dtos/library.dto.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { S3StorageRepository } from 'src/repositories/s3-storage.repository.js';
import { mimeTypes } from 'src/utils/mime-types.js';

export interface WatchEvents {
  onReady(): void;
  onAdd(path: string): void;
  onChange(path: string): void;
  onUnlink(path: string): void;
  onError(error: Error): void;
}

export type WatchOptions = Omit<ChokidarOptions, 'ignored'> & { ignored?: string[] };

export interface ImmichReadStream {
  stream: Readable;
  type?: string;
  disposition?: string | string[];
  length?: number;
  contentRange?: string;
}

export interface ImmichZipStream extends ImmichReadStream {
  addFile: (inputPath: string, filename: string) => void;
  finalize: () => Promise<void>;
}

export interface DiskUsage {
  available: number;
  free: number;
  total: number;
  used?: number;
  objectStorage?: boolean;
}

@Injectable()
export class StorageRepository {
  constructor(
    private logger: LoggingRepository,
    @Optional() private s3?: S3StorageRepository,
  ) {
    this.logger.setContext(StorageRepository.name);
  }

  isObjectStoragePath(filepath: string) {
    return this.s3 ? this.s3.managesPath(filepath) && !this.s3.isHlsPath(filepath) : false;
  }

  getLocalPath(filepath: string) {
    return this.s3?.localPath(filepath) ?? filepath;
  }

  async materializeFile(filepath: string, mode: 'read' | 'write' | 'readwrite') {
    if (!this.isObjectStoragePath(filepath)) {
      const localPath = this.getLocalPath(filepath);
      if (mode !== 'read') {
        await fs.mkdir(path.dirname(localPath), { recursive: true });
      }
      return { path: localPath, commit: async () => {}, cleanup: async () => {} };
    }

    const key = this.s3!.key(filepath);
    const temporaryFolder = await fs.mkdtemp(path.join(tmpdir(), 'immich-s3-'));
    const localPath = path.join(temporaryFolder, path.basename(filepath) || 'object');
    let metadata: Record<string, string> | undefined;
    try {
      if (mode !== 'write') {
        metadata = await this.downloadObject(key, localPath);
      }
    } catch (error) {
      await fs.rm(temporaryFolder, { recursive: true, force: true });
      throw error;
    }

    return {
      path: localPath,
      commit: async () => this.s3!.upload(key, createReadStream(localPath), metadata),
      cleanup: async () => fs.rm(temporaryFolder, { recursive: true, force: true }),
    };
  }

  async withLocalFile<T>(
    filepath: string,
    mode: 'read' | 'write' | 'readwrite',
    callback: (localPath: string) => Promise<T>,
  ): Promise<T> {
    const local = await this.materializeFile(filepath, mode);
    try {
      const result = await callback(local.path);
      if (mode !== 'read') {
        await local.commit();
      }
      return result;
    } finally {
      await local.cleanup();
    }
  }

  realpath(filepath: string) {
    if (this.isObjectStoragePath(filepath)) {
      return this.checkFileExists(filepath).then((exists) => {
        if (!exists) {
          throw this.noEntryError(filepath);
        }
        return filepath;
      });
    }
    return fs.realpath(this.getLocalPath(filepath));
  }

  readdir(folder: string): Promise<string[]> {
    if (this.isObjectStoragePath(folder)) {
      const prefix = `${this.s3!.key(folder).replace(/\/$/, '')}/`;
      return this.listDirectory(prefix);
    }
    return fs.readdir(this.getLocalPath(folder));
  }

  readdirWithTypes(folder: string): Promise<Dirent[]> {
    if (this.isObjectStoragePath(folder)) {
      return Promise.reject(new Error('Reading S3 directories with filesystem entry types is unsupported'));
    }
    return fs.readdir(this.getLocalPath(folder), { withFileTypes: true });
  }

  async copyFile(source: string, target: string) {
    const sourceIsS3 = this.isObjectStoragePath(source);
    const targetIsS3 = this.isObjectStoragePath(target);
    if (sourceIsS3 && targetIsS3) {
      const key = this.s3!.key(source);
      const info = await this.s3!.headObject(key);
      if ((info.ContentLength ?? 0) <= 5_000_000_000) {
        await this.s3!.copy(key, this.s3!.key(target));
        return;
      }
      const { Body, Metadata } = await this.s3!.getObject(key);
      if (!Body) {
        throw this.noEntryError(source);
      }
      await this.s3!.upload(this.s3!.key(target), Body as Readable, Metadata);
      return;
    }

    if (sourceIsS3) {
      const { Body } = await this.s3!.getObject(this.s3!.key(source));
      if (!Body) {
        throw this.noEntryError(source);
      }
      await pipeline(Body as Readable, createWriteStream(this.getLocalPath(target)));
      return;
    }

    if (targetIsS3) {
      await this.s3!.upload(this.s3!.key(target), createReadStream(this.getLocalPath(source)));
      return;
    }

    return fs.copyFile(this.getLocalPath(source), this.getLocalPath(target));
  }

  async stat(filepath: string): Promise<Stats> {
    if (this.isObjectStoragePath(filepath)) {
      const key = this.s3!.key(filepath);
      try {
        const result = await this.s3!.headObject(key);
        const mtime = this.parseObjectDate(result.Metadata?.['immich-mtime']) ?? result.LastModified ?? new Date();
        const atime = this.parseObjectDate(result.Metadata?.['immich-atime']) ?? mtime;
        return this.objectStats(Number(result.ContentLength ?? 0), atime, mtime, false);
      } catch (error) {
        if (!this.isNotFoundError(error)) {
          throw error;
        }

        const directory = await this.s3!.listPage(`${key.replace(/\/$/, '')}/`, undefined, 1);
        if ((directory.Contents?.length ?? 0) > 0 || (directory.CommonPrefixes?.length ?? 0) > 0) {
          return this.objectStats(0, new Date(0), new Date(0), true);
        }

        throw this.noEntryError(filepath);
      }
    }
    return fs.stat(this.getLocalPath(filepath));
  }

  async createFile(filepath: string, buffer: Buffer) {
    if (this.isObjectStoragePath(filepath)) {
      if (await this.checkFileExists(filepath)) {
        throw Object.assign(new Error(`File already exists: ${filepath}`), { code: 'EEXIST' });
      }
      return this.s3!.putObject(this.s3!.key(filepath), buffer);
    }
    return fs.writeFile(this.getLocalPath(filepath), buffer, { flag: 'wx' });
  }

  createWriteStream(filepath: string): Writable {
    if (this.isObjectStoragePath(filepath)) {
      return this.createS3WriteStream(filepath);
    }
    return createWriteStream(this.getLocalPath(filepath), { flags: 'w', flush: true });
  }

  createOrOverwriteFile(filepath: string, buffer: Buffer) {
    if (this.isObjectStoragePath(filepath)) {
      return this.s3!.putObject(this.s3!.key(filepath), buffer);
    }
    return fs.writeFile(this.getLocalPath(filepath), buffer, { flag: 'w' });
  }

  overwriteFile(filepath: string, buffer: Buffer) {
    if (this.isObjectStoragePath(filepath)) {
      return this.s3!.putObject(this.s3!.key(filepath), buffer);
    }
    return fs.writeFile(this.getLocalPath(filepath), buffer, { flag: 'r+' });
  }

  async rename(source: string, target: string) {
    if (this.isObjectStoragePath(source) && this.isObjectStoragePath(target)) {
      await this.copyFile(source, target);
      await this.unlink(source);
      return;
    }

    if (this.isObjectStoragePath(source)) {
      await this.copyFile(source, target);
      await this.unlink(source);
      return;
    }

    if (this.isObjectStoragePath(target)) {
      await this.copyFile(source, target);
      await fs.unlink(this.getLocalPath(source));
      return;
    }

    return fs.rename(this.getLocalPath(source), this.getLocalPath(target));
  }

  utimes(filepath: string, atime: Date, mtime: Date) {
    if (this.isObjectStoragePath(filepath)) {
      return this.s3!.setTimes(this.s3!.key(filepath), atime, mtime);
    }
    return fs.utimes(this.getLocalPath(filepath), atime, mtime);
  }

  createZipStream(): ImmichZipStream {
    const archive = archiver('zip', { store: true });

    const addFile = (input: string, filename: string) => {
      if (this.isObjectStoragePath(input)) {
        archive.append(this.createPlainReadStream(input), { name: filename, mode: 0o644 });
      } else {
        archive.file(this.getLocalPath(input), { name: filename, mode: 0o644 });
      }
    };

    const finalize = () => archive.finalize();

    return { stream: archive, addFile, finalize };
  }

  createGzip(): PassThrough {
    return createGzip();
  }

  createGunzip(): PassThrough {
    return createGunzip();
  }

  createPlainReadStream(filepath: string, range?: { start: number; end: number }): Readable {
    if (this.isObjectStoragePath(filepath)) {
      const output = new PassThrough();
      void this.s3!.getObject(this.s3!.key(filepath), range ? `bytes=${range.start}-${range.end}` : undefined)
        .then(({ Body }) => {
          if (!Body) {
            output.destroy(this.noEntryError(filepath));
            return;
          }
          const body = Body as Readable;
          body.on('error', (error) => output.destroy(error));
          output.on('close', () => body.destroy());
          body.pipe(output);
        })
        .catch((error) => output.destroy(error as Error));
      return output;
    }
    return createReadStream(this.getLocalPath(filepath));
  }

  async createReadStream(
    filepath: string,
    mimeType?: string | null,
    range?: { start: number; end: number },
  ): Promise<ImmichReadStream> {
    if (this.isObjectStoragePath(filepath)) {
      const stats = await this.stat(filepath);
      await this.checkReadable(filepath);
      return {
        stream: this.createPlainReadStream(filepath, range),
        length: range ? range.end - range.start + 1 : stats.size,
        type: mimeType || undefined,
        contentRange: range ? `bytes ${range.start}-${range.end}/${stats.size}` : undefined,
      };
    }
    const localPath = this.getLocalPath(filepath);
    const { size } = await fs.stat(localPath);
    await fs.access(localPath, constants.R_OK);
    return {
      stream: createReadStream(this.getLocalPath(filepath), range),
      length: size,
      type: mimeType || undefined,
    };
  }

  async readFile(filepath: string, options?: ReadOptionsWithBuffer<Buffer>): Promise<Buffer> {
    if (this.isObjectStoragePath(filepath)) {
      const key = this.s3!.key(filepath);
      const start = Number(options?.position ?? 0);
      const range =
        options?.length && options.position !== null ? `bytes=${start}-${start + options.length - 1}` : undefined;
      const { Body } = await this.s3!.getObject(key, range);
      if (!Body) {
        throw this.noEntryError(filepath);
      }
      const bytes = Buffer.from(await Body.transformToByteArray());
      if (!options?.buffer) {
        return bytes;
      }
      bytes.copy(options.buffer, options.offset ?? 0, 0, options.length ?? bytes.length);
      return options.buffer.subarray(options.offset ?? 0, (options.offset ?? 0) + bytes.length);
    }

    // read a slice
    if (options) {
      const file = await fs.open(this.getLocalPath(filepath));
      try {
        const { buffer } = await file.read(options);
        return buffer as Buffer;
      } finally {
        await file.close();
      }
    }

    // read everything
    return fs.readFile(this.getLocalPath(filepath));
  }

  async readJsonFile<T>(filepath: string): Promise<T> {
    const file = await this.readFile(filepath).then((data) => data.toString('utf8'));
    return JSON.parse(file) as T;
  }

  async checkFileExists(filepath: string, mode = constants.F_OK): Promise<boolean> {
    if (this.isObjectStoragePath(filepath)) {
      try {
        await this.s3!.headObject(this.s3!.key(filepath));
        return true;
      } catch (error) {
        if (this.isNotFoundError(error)) {
          return false;
        }
        throw error;
      }
    }
    try {
      await fs.access(this.getLocalPath(filepath), mode);
      return true;
    } catch {
      return false;
    }
  }

  async unlink(file: string) {
    if (this.isObjectStoragePath(file)) {
      await this.s3!.delete(this.s3!.key(file));
      return;
    }
    try {
      await fs.unlink(this.getLocalPath(file));
    } catch (error) {
      if ((error as NodeJS.ErrnoException)?.code === 'ENOENT') {
        this.logger.warn(`File ${file} does not exist.`);
      } else {
        throw error;
      }
    }
  }

  async unlinkDir(folder: string, options: { recursive?: boolean; force?: boolean }) {
    if (this.isObjectStoragePath(folder)) {
      const prefix = `${this.s3!.key(folder).replace(/\/$/, '')}/`;
      if (!options.recursive) {
        const page = await this.s3!.listPage(prefix, undefined, 1);
        if ((page.Contents?.length ?? 0) > 0) {
          throw Object.assign(new Error(`Directory not empty: ${folder}`), { code: 'ENOTEMPTY' });
        }
        return;
      }
      await this.s3!.removePrefix(prefix);
      return;
    }
    await fs.rm(this.getLocalPath(folder), { ...options, maxRetries: 5, retryDelay: 100 });
  }

  async removeEmptyDirs(directory: string, self: boolean = false) {
    if (this.isObjectStoragePath(directory)) {
      return;
    }
    directory = this.getLocalPath(directory);
    // lstat does not follow symlinks (in contrast to stat)
    const stats = await fs.lstat(directory);
    if (!stats.isDirectory()) {
      return;
    }

    const files = await fs.readdir(directory);
    await Promise.all(files.map((file) => this.removeEmptyDirs(path.join(directory, file), true)));

    if (!self) {
      return;
    }

    const updated = await fs.readdir(directory);
    if (updated.length === 0) {
      try {
        await fs.rmdir(directory);
      } catch (error: Error | any) {
        if (error.code !== 'ENOTEMPTY') {
          this.logger.warn(`Attempted to remove directory, but failed: ${error}`);
        }
      }
    }
  }

  mkdirSync(filepath: string): void {
    if (this.isObjectStoragePath(filepath)) {
      return;
    }
    filepath = this.getLocalPath(filepath);
    if (!existsSync(filepath)) {
      mkdirSync(filepath, { recursive: true });
    }
  }

  existsSync(filepath: string) {
    if (this.isObjectStoragePath(filepath)) {
      return true;
    }
    filepath = this.getLocalPath(filepath);
    return existsSync(filepath);
  }

  async checkDiskUsage(folder: string): Promise<DiskUsage> {
    if (this.isObjectStoragePath(folder)) {
      return { available: 0, free: 0, total: 0, objectStorage: true };
    }
    const stats = await fs.statfs(this.getLocalPath(folder));
    return {
      available: stats.bavail * stats.frsize,
      free: stats.bfree * stats.frsize,
      total: stats.blocks * stats.frsize,
    };
  }

  crawl(crawlOptions: CrawlOptionsDto): Promise<string[]> {
    const { pathsToCrawl, exclusionPatterns, includeHidden } = crawlOptions;
    if (pathsToCrawl.length === 0) {
      return Promise.resolve([]);
    }

    if (pathsToCrawl.some((path) => this.isObjectStoragePath(path))) {
      return this.crawlS3(crawlOptions);
    }

    const globbedPaths = pathsToCrawl.map((path) => this.asGlob(path));

    // eslint-disable-next-line import-x/no-named-as-default-member
    return fastGlob.glob(globbedPaths, {
      absolute: true,
      caseSensitiveMatch: false,
      onlyFiles: true,
      dot: includeHidden,
      ignore: exclusionPatterns,
    });
  }

  async *walk(walkOptions: WalkOptionsDto): AsyncGenerator<string[]> {
    const { pathsToCrawl, exclusionPatterns, includeHidden } = walkOptions;
    if (pathsToCrawl.length === 0) {
      async function* emptyGenerator() {}
      return emptyGenerator();
    }

    if (pathsToCrawl.some((path) => this.isObjectStoragePath(path))) {
      yield* this.walkS3(walkOptions);
      return;
    }

    const globbedPaths = pathsToCrawl.map((path) => this.asGlob(path));

    // eslint-disable-next-line import-x/no-named-as-default-member
    const stream = fastGlob.globStream(globbedPaths, {
      absolute: true,
      caseSensitiveMatch: false,
      onlyFiles: true,
      dot: includeHidden,
      ignore: exclusionPatterns,
    });

    let batch: string[] = [];
    for await (const value of stream) {
      batch.push(value.toString());
      if (batch.length !== walkOptions.take) {
        continue;
      }

      yield batch;
      batch = [];
    }

    if (batch.length > 0) {
      yield batch;
    }
  }

  watch(paths: string[], options: WatchOptions, events: Partial<WatchEvents>) {
    if (paths.some((path) => this.isObjectStoragePath(path))) {
      throw new Error('S3-backed folders cannot be watched as external libraries');
    }
    const matchesIgnoredPath = picomatch(options.ignored ?? [], {
      dot: true, // Match the behavior of fast-glob's micromatch by using these settings
      nocase: true,
      posix: true,
      strictSlashes: false,
    });

    const watcher = chokidarWatch(
      paths.map((path) => this.getLocalPath(path)),
      {
        ...options,
        ignored: (path) => matchesIgnoredPath(path),
      },
    );

    watcher.on('ready', () => events.onReady?.());
    watcher.on('add', (path) => events.onAdd?.(path));
    watcher.on('change', (path) => events.onChange?.(path));
    watcher.on('unlink', (path) => events.onUnlink?.(path));
    watcher.on('error', (error) => events.onError?.(error as Error));

    return () => watcher.close();
  }

  watchDir(filepath: string, listener: (eventType: string, filename: string | null) => void) {
    return watch(this.getLocalPath(filepath), listener); // Native fs.watch without chokidar overhead
  }

  private asGlob(pathToCrawl: string): string {
    // eslint-disable-next-line import-x/no-named-as-default-member
    const escapedPath = fastGlob
      .escapePath(pathToCrawl)
      .replaceAll('"', '["]')
      .replaceAll("'", "[']")
      .replaceAll('`', '[`]');
    const extensions = `*{${mimeTypes.getSupportedFileExtensions().join(',')}}`;
    return `${escapedPath}/**/${extensions}`;
  }

  private async listDirectory(prefix: string) {
    const names = new Set<string>();
    for await (const { contents, prefixes } of this.s3!.listDirectory(prefix)) {
      for (const object of contents) {
        if (!object.Key || object.Key === prefix) {
          continue;
        }
        const name = object.Key.slice(prefix.length).replace(/\/$/, '');
        if (name && !name.includes('/')) {
          names.add(name);
        }
      }
      for (const { Prefix: childPrefix } of prefixes) {
        if (!childPrefix) {
          continue;
        }
        const name = childPrefix.slice(prefix.length).replace(/\/$/, '');
        if (name) {
          names.add(name);
        }
      }
    }
    return [...names];
  }

  private async crawlS3(options: CrawlOptionsDto) {
    const paths = options.pathsToCrawl;
    const localPaths = paths.filter((path) => !this.isObjectStoragePath(path));
    const remotePaths = paths.filter((path) => this.isObjectStoragePath(path));
    const files = localPaths.length > 0 ? await this.crawl({ ...options, pathsToCrawl: localPaths }) : [];
    for await (const batch of this.walkS3({ ...options, pathsToCrawl: remotePaths, take: Number.MAX_SAFE_INTEGER })) {
      files.push(...batch);
    }
    return files;
  }

  private async *walkS3(options: WalkOptionsDto): AsyncGenerator<string[]> {
    const { pathsToCrawl, includeHidden = false } = options;
    const exclusionPatterns = options.exclusionPatterns ?? [];
    const localPaths = pathsToCrawl.filter((path) => !this.isObjectStoragePath(path));
    const remotePaths = pathsToCrawl.filter((path) => this.isObjectStoragePath(path));
    if (localPaths.length > 0) {
      yield* this.walk({ ...options, pathsToCrawl: localPaths });
    }

    const supportedExtensions = new Set(mimeTypes.getSupportedFileExtensions());
    const isExcluded = (filePath: string) =>
      exclusionPatterns.some((pattern) => picomatch(pattern, { dot: true, nocase: true })(filePath));
    let batch: string[] = [];
    for (const folder of remotePaths) {
      const basePath = this.s3!.key(folder).replace(/\/$/, '');
      const prefix = basePath ? `${basePath}/` : '';
      for await (const objects of this.s3!.list(prefix)) {
        for (const { Key } of objects) {
          if (!Key || Key.endsWith('/')) {
            continue;
          }

          const filePath = this.s3!.path(Key);
          const relativePath = path.relative(folder, filePath);
          if (
            !supportedExtensions.has(path.extname(filePath).toLowerCase()) ||
            (!includeHidden && relativePath.split(path.sep).some((part) => part.startsWith('.'))) ||
            isExcluded(filePath) ||
            isExcluded(relativePath)
          ) {
            continue;
          }

          batch.push(filePath);
          if (batch.length !== options.take) {
            continue;
          }
          yield batch;
          batch = [];
        }
      }
    }

    if (batch.length > 0) {
      yield batch;
    }
  }

  private createS3WriteStream(filepath: string): Writable {
    const body = new PassThrough();
    const key = this.s3!.key(filepath);
    const upload = this.s3!.upload(key, body);
    const output = new Writable({
      write(chunk, encoding, callback) {
        if (body.write(chunk, encoding)) {
          callback();
          return;
        }
        const onError = (error: Error) => {
          body.off('drain', onDrain);
          callback(error);
        };
        const onDrain = () => {
          body.off('error', onError);
          callback();
        };
        body.once('drain', onDrain);
        body.once('error', onError);
      },
      final(callback) {
        body.end();
        upload.then(() => callback()).catch(callback);
      },
      destroy(error, callback) {
        if (error) {
          body.destroy(error);
        } else {
          body.destroy();
        }
        callback(error);
      },
    });
    upload.catch((error) => output.destroy(error as Error));
    return output;
  }

  private async downloadObject(key: string, filepath: string) {
    const { Body, Metadata } = await this.s3!.getObject(key);
    if (!Body) {
      throw this.noEntryError(filepath);
    }
    await pipeline(Body as Readable, createWriteStream(filepath));
    return Metadata;
  }

  private async checkReadable(filepath: string) {
    if (this.isObjectStoragePath(filepath)) {
      return;
    }
    await fs.access(this.getLocalPath(filepath), constants.R_OK);
  }

  private isNotFoundError(error: unknown) {
    const candidate = error as { name?: string; code?: string; Code?: string; $metadata?: { httpStatusCode?: number } };
    return (
      candidate?.$metadata?.httpStatusCode === 404 ||
      ['NoSuchKey', 'NotFound', 'NoSuchBucket'].includes(candidate?.name ?? candidate?.code ?? candidate?.Code ?? '')
    );
  }

  private noEntryError(filepath: string) {
    return Object.assign(new Error(`No such file or directory: ${filepath}`), { code: 'ENOENT' });
  }

  private parseObjectDate(value?: string) {
    if (!value) {
      return;
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? undefined : date;
  }

  private objectStats(size: number, atime: Date, mtime: Date, isDirectory: boolean): Stats {
    const mode = isDirectory ? 0o4_0755 : 0o10_0644;
    return {
      dev: 0,
      mode,
      nlink: 1,
      uid: 0,
      gid: 0,
      rdev: 0,
      blksize: 0,
      ino: 0,
      size,
      blocks: Math.ceil(size / 512),
      atimeMs: atime.getTime(),
      mtimeMs: mtime.getTime(),
      ctimeMs: mtime.getTime(),
      birthtimeMs: mtime.getTime(),
      atime,
      mtime,
      ctime: mtime,
      birthtime: mtime,
      isFile: () => !isDirectory,
      isDirectory: () => isDirectory,
      isBlockDevice: () => false,
      isCharacterDevice: () => false,
      isSymbolicLink: () => false,
      isFIFO: () => false,
      isSocket: () => false,
    } as Stats;
  }
}
