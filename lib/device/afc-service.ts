/**
 * AFC (Apple File Conduit) Service adapter module.
 *
 * This module provides a unified interface for AFC operations that works with both:
 * - appium-ios-device (iOS < 18)
 * - appium-ios-remotexpc (iOS >= 18)
 *
 * The adapter ensures API compatibility between the two implementations while
 * leveraging the appropriate service based on iOS version.
 */

import {services} from 'appium-ios-device';
import {Readable, Writable} from 'stream';
import {log} from '../logger';
import {isIos18OrNewer} from '../utils';
import type {XCUITestDriverOpts} from '../driver';

/**
 * FileInfo interface matching appium-ios-device's FileInfo class
 */
export interface FileInfo {
  size: number;
  blocks: number;
  nlink: number;
  ifmt: string;
  mtimeMs: number;
  birthtimeMs: number;
  isDirectory: () => boolean;
  isFile: () => boolean;
}

/**
 * AFC Service interface that abstracts the differences between
 * appium-ios-device and appium-ios-remotexpc AFC implementations.
 */
export interface AfcServiceAdapter {
  /** List directory contents */
  listDirectory: (path: string) => Promise<string[]>;
  /** Create a directory (recursive) */
  createDirectory: (path: string) => Promise<void>;
  /** Delete a directory and its contents */
  deleteDirectory: (path: string) => Promise<void>;
  /** Get file/directory info */
  getFileInfo: (path: string) => Promise<FileInfo>;
  /** Walk directory tree with callback */
  walkDir: (dir: string, recursive: boolean, onPath: (itemPath: string, isDirectory: boolean) => Promise<void>) => Promise<void>;
  /** Create a readable stream for a file */
  createReadStream: (filePath: string, opts?: {autoDestroy?: boolean}) => Promise<Readable>;
  /** Create a writable stream for a file */
  createWriteStream: (filePath: string, opts?: {autoDestroy?: boolean}) => Promise<Writable>;
  /**
   * Read entire file contents into a buffer.
   * This is a synchronous operation that properly waits for the file handle to close
   * before returning, making it safe for sequential file operations.
   */
  getFileContents: (filePath: string) => Promise<Buffer>;
  /** Close the service connection */
  close: () => void;
  /**
   * Whether this adapter requires serial (non-concurrent) operations.
   * RemoteXPC AFC uses a single socket that cannot handle interleaved operations.
   */
  readonly requiresSerialOperations: boolean;
}

/**
 * Adapter class that wraps appium-ios-remotexpc AFC service to match
 * the appium-ios-device AFC service interface.
 */
class RemoteXpcAfcAdapter implements AfcServiceAdapter {
  private afcService: any;

  /**
   * RemoteXPC AFC uses a single socket and cannot handle interleaved operations.
   * File operations must be serialized to avoid protocol corruption.
   */
  readonly requiresSerialOperations = true;

  constructor(afcService: any) {
    this.afcService = afcService;
  }

  async listDirectory(dirPath: string): Promise<string[]> {
    return await this.afcService.listdir(dirPath);
  }

  async createDirectory(dirPath: string): Promise<void> {
    await this.afcService.mkdir(dirPath);
  }

  async deleteDirectory(dirPath: string): Promise<void> {
    await this.afcService.rm(dirPath, true);
  }

  async getFileInfo(filePath: string): Promise<FileInfo> {
    const stat = await this.afcService.stat(filePath);

    // Map remotexpc StatInfo to FileInfo interface
    return {
      size: Number(stat.st_size),
      blocks: stat.st_blocks,
      nlink: stat.st_nlink,
      ifmt: stat.st_ifmt,
      mtimeMs: stat.st_mtime.getTime(),
      birthtimeMs: stat.st_birthtime.getTime(),
      isDirectory: () => stat.st_ifmt === 'S_IFDIR',
      isFile: () => stat.st_ifmt === 'S_IFREG',
    };
  }

  async walkDir(
    dir: string,
    recursive: boolean,
    onPath: (itemPath: string, isDirectory: boolean) => Promise<void>
  ): Promise<void> {
    const entries = await this.afcService.listdir(dir);

    for (const entry of entries) {
      // Skip . and .. entries
      if (entry === '.' || entry === '..') {
        continue;
      }

      const fullPath = dir.endsWith('/') ? `${dir}${entry}` : `${dir}/${entry}`;

      // Use stat() instead of isdir() to avoid state corruption issues
      // when read streams are active
      const fileInfo = await this.getFileInfo(fullPath);
      const isDir = fileInfo.isDirectory();

      await onPath(fullPath, isDir);

      if (isDir && recursive) {
        await this.walkDir(fullPath, recursive, onPath);
      }
    }
  }

  async createReadStream(filePath: string, opts?: {autoDestroy?: boolean}): Promise<Readable> {
    // remotexpc's readToStream returns a Readable stream
    const stream = await this.afcService.readToStream(filePath);

    if (opts?.autoDestroy) {
      // The stream from remotexpc handles cleanup via 'close' event on the handle
      // This is already handled in the remotexpc implementation
    }

    return stream;
  }

  async getFileContents(filePath: string): Promise<Buffer> {
    return await this.afcService.getFileContents(filePath);
  }

  async createWriteStream(filePath: string, opts?: {autoDestroy?: boolean}): Promise<Writable> {
    // For remotexpc, we need to open the file and create a write stream
    const handle = await this.afcService.fopen(filePath, 'w');
    const writeStream = this.afcService.createWriteStream(handle);

    // Handle cleanup when stream finishes
    if (opts?.autoDestroy) {
      const closeHandle = async () => {
        try {
          await this.afcService.fclose(handle);
        } catch {
          // Ignore close errors
        }
      };
      writeStream.on('finish', closeHandle);
      writeStream.on('error', closeHandle);
    }

    return writeStream;
  }

  close(): void {
    this.afcService.close();
  }
}

/**
 * Adapter class that wraps appium-ios-device AFC service.
 * This is a pass-through adapter since the existing code already uses this interface.
 */
class IosDeviceAfcAdapter implements AfcServiceAdapter {
  private afcService: any;

  /**
   * appium-ios-device uses a proper protocol handler that can handle concurrent operations.
   */
  readonly requiresSerialOperations = false;

  constructor(afcService: any) {
    this.afcService = afcService;
  }

  async listDirectory(dirPath: string): Promise<string[]> {
    return await this.afcService.listDirectory(dirPath);
  }

  async createDirectory(dirPath: string): Promise<void> {
    await this.afcService.createDirectory(dirPath);
  }

  async deleteDirectory(dirPath: string): Promise<void> {
    await this.afcService.deleteDirectory(dirPath);
  }

  async getFileInfo(filePath: string): Promise<FileInfo> {
    return await this.afcService.getFileInfo(filePath);
  }

  async walkDir(
    dir: string,
    recursive: boolean,
    onPath: (itemPath: string, isDirectory: boolean) => Promise<void>
  ): Promise<void> {
    await this.afcService.walkDir(dir, recursive, onPath);
  }

  async createReadStream(filePath: string, opts?: {autoDestroy?: boolean}): Promise<Readable> {
    return await this.afcService.createReadStream(filePath, opts);
  }

  async createWriteStream(filePath: string, opts?: {autoDestroy?: boolean}): Promise<Writable> {
    return await this.afcService.createWriteStream(filePath, opts);
  }

  async getFileContents(filePath: string): Promise<Buffer> {
    // For appium-ios-device, use the stream approach since it handles concurrency properly
    const stream = await this.afcService.createReadStream(filePath, {autoDestroy: true});
    const chunks: Buffer[] = [];
    return new Promise((resolve, reject) => {
      stream.on('data', (chunk: Buffer) => chunks.push(chunk));
      stream.on('end', () => resolve(Buffer.concat(chunks)));
      stream.on('error', reject);
    });
  }

  close(): void {
    this.afcService.close();
  }
}

/**
 * Start AFC service with the appropriate implementation based on iOS version.
 *
 * @param udid - Device UDID
 * @param opts - Driver options containing platformVersion
 * @returns AFC service adapter with unified interface
 */
export async function startAfcService(
  udid: string,
  opts?: XCUITestDriverOpts
): Promise<AfcServiceAdapter> {
  if (opts && isIos18OrNewer(opts)) {
    log.debug(`Using appium-ios-remotexpc AFC service for iOS 18+ device ${udid}`);
    try {
      const {Services} = await import('appium-ios-remotexpc');
      const afcService = await Services.startAfcService(udid);
      return new RemoteXpcAfcAdapter(afcService);
    } catch (err: any) {
      log.warn(
        `Failed to start remotexpc AFC service, falling back to appium-ios-device: ${err.message}`
      );
      // Fall back to appium-ios-device if remotexpc fails
      const afcService = await services.startAfcService(udid);
      return new IosDeviceAfcAdapter(afcService);
    }
  }

  log.debug(`Using appium-ios-device AFC service for device ${udid}`);
  const afcService = await services.startAfcService(udid);
  return new IosDeviceAfcAdapter(afcService);
}

export {RemoteXpcAfcAdapter, IosDeviceAfcAdapter};
