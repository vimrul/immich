import { NotFoundException, StreamableFile } from '@nestjs/common';
import { NextFunction, Response } from 'express';
import { access, constants } from 'node:fs/promises';
import { basename, extname } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { promisify } from 'node:util';
import { CacheControl } from 'src/enum.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import { ImmichReadStream, StorageRepository } from 'src/repositories/storage.repository.js';
import { onRouteError } from 'src/utils/logger.js';

export function getFileNameWithoutExtension(path: string): string {
  return basename(path, getFilenameExtension(path));
}

export function getFilenameExtension(path: string) {
  const extension = extname(path);
  if (!extension && path.startsWith('.') && !path.includes('.', 1)) {
    return path;
  }
  return extension;
}

export function getLivePhotoMotionFilename(stillName: string, motionName: string) {
  return getFileNameWithoutExtension(stillName) + getFilenameExtension(motionName);
}

export class ImmichFileResponse {
  public readonly path!: string;
  public readonly contentType!: string;
  public readonly cacheControl!: CacheControl;
  public readonly fileName?: string;

  constructor(response: ImmichFileResponse) {
    Object.assign(this, response);
  }
}
type SendFile = Parameters<Response['sendFile']>;
type SendFileOptions = SendFile[1];

const cacheControlHeaders: Record<CacheControl, string | null> = {
  [CacheControl.PrivateWithCache]:
    'private, max-age=86400, no-transform, stale-while-revalidate=2592000, stale-if-error=2592000',
  [CacheControl.PrivateWithoutCache]: 'private, no-cache, no-transform',
  [CacheControl.None]: null, // falsy value to prevent adding Cache-Control header
};

export const sendFile = async (
  res: Response,
  next: NextFunction,
  handler: () => Promise<ImmichFileResponse> | ImmichFileResponse,
  logger: LoggingRepository,
  storageRepository?: StorageRepository,
): Promise<void> => {
  // promisified version of 'res.sendFile' for cleaner async handling
  const _sendFile = (path: string, options: SendFileOptions) =>
    promisify<string, SendFileOptions>(res.sendFile).bind(res)(path, options);

  try {
    const file = await handler();

    if (storageRepository?.isObjectStoragePath(file.path)) {
      const stats = await storageRepository.stat(file.path);
      const rangeHeader = res.req.headers['if-range'] ? undefined : res.req.headers.range;
      const range = rangeHeader ? parseByteRange(rangeHeader, stats.size) : null;
      if (range === 'unsatisfiable') {
        res.setHeader('Content-Range', `bytes */${stats.size}`);
        res.status(416).end();
        return;
      }

      const response = await storageRepository.createReadStream(file.path, file.contentType, range ?? undefined);
      res.setHeader('Accept-Ranges', 'bytes');
      res.setHeader('Content-Type', file.contentType);
      if (file.fileName) {
        res.setHeader('Content-Disposition', `inline; filename*=UTF-8''${encodeURIComponent(file.fileName)}`);
      }
      const cacheControlHeader = cacheControlHeaders[file.cacheControl];
      if (cacheControlHeader) {
        res.setHeader('Cache-Control', cacheControlHeader);
      }
      if (response.contentRange) {
        res.status(206);
        res.setHeader('Content-Range', response.contentRange);
      }
      if (response.length !== undefined) {
        res.setHeader('Content-Length', response.length);
      }
      await pipeline(response.stream, res);
      return;
    }

    const localPath = storageRepository?.getLocalPath(file.path) ?? file.path;
    await access(localPath, constants.R_OK);

    const cacheControlHeader = cacheControlHeaders[file.cacheControl];
    if (cacheControlHeader) {
      // set the header to Cache-Control
      res.header('Cache-Control', cacheControlHeader);
    }

    res.header('Content-Type', file.contentType);
    if (file.fileName) {
      res.header('Content-Disposition', `inline; filename*=UTF-8''${encodeURIComponent(file.fileName)}`);
    }

    return await _sendFile(localPath, { dotfiles: 'allow' });
  } catch (error: Error | any) {
    const { canWrite } = onRouteError(undefined, res, error, logger);
    if (canWrite) {
      next(new NotFoundException());
    }
  }
};

const parseByteRange = (header: string, size: number): 'unsatisfiable' | null | { start: number; end: number } => {
  const match = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!match || size <= 0 || (!match[1] && !match[2])) {
    return match ? 'unsatisfiable' : null;
  }

  const hasStart = match[1] !== '';
  const hasEnd = match[2] !== '';
  let start = hasStart ? Number(match[1]) : NaN;
  let end = hasEnd ? Number(match[2]) : NaN;
  if (hasStart) {
    end = hasEnd ? Math.min(end, size - 1) : size - 1;
  } else {
    const suffixLength = end;
    if (!suffixLength) {
      return 'unsatisfiable';
    }
    start = Math.max(0, size - suffixLength);
    end = size - 1;
  }

  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || start >= size || end < start) {
    return 'unsatisfiable';
  }
  return { start, end };
};

export const asStreamableFile = ({ stream, type, disposition, length }: ImmichReadStream) => {
  return new StreamableFile(stream, { type, disposition, length });
};
