export * as ReadToolFileSystem from "./read-filesystem.js"

import path from "path"
import { pathToFileURL } from "url"
import { makeLocationNode } from "@opencode/util/effect/app-node"
import { Context, Effect, Layer, Schema } from "effect"
import { lookup } from "mime-types"
import { Environment } from "../environment/index.js"
import type { Files } from "../environment/index.js"
import { FileSystem } from "../filesystem.js"
import { Mime } from "../mime.js"
import { AbsolutePath, NonNegativeInt, PositiveInt, RelativePath } from "../schema.js"

export const MAX_READ_LINES = 2_000
export const MAX_READ_BYTES = 50 * 1024
export const MAX_MEDIA_INGEST_BYTES = 20 * 1024 * 1024
const FIRST_CHUNK = 256 * 1024
const TAIL_LINES = 100
const MAX_LINE_LENGTH = 2_000
const MAX_LINE_SUFFIX = `... (line truncated to ${MAX_LINE_LENGTH} chars)`
export const MEDIA_MIMES = new Set(["image/png", "image/jpeg", "image/gif", "image/webp", "application/pdf"])

export class BinaryFileError extends Schema.TaggedError<BinaryFileError>()("ReadTool.BinaryFileError", {
  resource: Schema.String,
}) {
  override get message() {
    return `Cannot read binary file: ${this.resource}`
  }
}

export class MediaIngestLimitError extends Schema.TaggedError<MediaIngestLimitError>()(
  "ReadTool.MediaIngestLimitError",
  {
    resource: Schema.String,
    maximumBytes: Schema.Number,
  },
) {
  override get message() {
    return `Media exceeds ${this.maximumBytes} byte ingestion limit: ${this.resource}`
  }
}

export class PathKindError extends Schema.TaggedError<PathKindError>()("ReadTool.PathKindError", {
  resource: Schema.String,
  expected: Schema.Literals(["a file", "a file or directory"]),
}) {
  override get message() {
    return `Path is not ${this.expected}: ${this.resource}`
  }
}

export class DirectoryOffsetError extends Schema.TaggedError<DirectoryOffsetError>()("ReadTool.DirectoryOffsetError", {
  resource: Schema.String,
}) {
  override get message() {
    return `A negative offset only works for files, and ${this.resource} is a directory`
  }
}

export type ReadError =
  | Environment.NotFound
  | Environment.Failed
  | BinaryFileError
  | MediaIngestLimitError
  | PathKindError
  | DirectoryOffsetError

export const PageInput = Schema.Struct({
  offset: Schema.optionalKey(Schema.Int),
  limit: Schema.optionalKey(NonNegativeInt),
})
export type PageInput = typeof PageInput.Type

export const FileContent = Schema.Struct({
  type: Schema.Literal("file"),
  ...FileSystem.Content.fields,
}).annotate({ identifier: "ReadTool.FileContent" })
export type FileContent = typeof FileContent.Type

export class TextPage extends Schema.Class<TextPage>("ReadTool.TextPage")({
  type: Schema.Literal("text-page"),
  content: Schema.String,
  mime: Schema.String,
  offset: PositiveInt,
  truncated: Schema.Boolean,
  next: Schema.optionalKey(PositiveInt),
}) {}

export interface ListEntry extends Schema.Schema.Type<typeof ListEntry> {}
export const ListEntry = Schema.Struct({
  path: RelativePath,
  type: Schema.Literals(["file", "directory", "symlink"]),
}).annotate({ identifier: "ReadTool.ListEntry" })

export class ListPage extends Schema.Class<ListPage>("ReadTool.ListPage")({
  type: Schema.Literal("list-page"),
  entries: Schema.Array(ListEntry),
  truncated: Schema.Boolean,
  next: Schema.optionalKey(PositiveInt),
}) {}

export interface Interface {
  readonly list: (path: AbsolutePath) => ReturnType<Files["list"]>
  readonly read: (
    path: AbsolutePath,
    resource: string,
    page?: PageInput,
  ) => Effect.Effect<FileContent | TextPage | ListPage, ReadError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ReadToolFileSystem") {}

const mimeType = (value: string) => lookup(value) || "application/octet-stream"

export const read = Effect.fn("ReadTool.read")(function* (
  files: Files,
  input: AbsolutePath,
  resource: string,
  page: PageInput = {},
) {
  const virtual = resource.startsWith("skill://")
  const first = yield* files.read(input, { offset: 0, length: FIRST_CHUNK }).pipe(
    Effect.catchTag("Environment.WrongKind", (error) =>
      Effect.gen(function* () {
        if (error.actual !== "directory")
          return yield* new PathKindError({ resource, expected: "a file or directory" })
        if (page.offset !== undefined && page.offset < 0) return yield* new DirectoryOffsetError({ resource })
        return yield* files.list(input).pipe(
          Effect.map((entries) => list(entries, page, virtual ? "/" : path.sep)),
          Effect.catchTag("Environment.WrongKind", () =>
            Effect.fail(new PathKindError({ resource, expected: "a file or directory" })),
          ),
        )
      }),
    ),
  )
  if (first instanceof ListPage) return first

  const media = Mime.detect(first.bytes)
  if (MEDIA_MIMES.has(media)) {
    if (first.info.size > MAX_MEDIA_INGEST_BYTES)
      return yield* new MediaIngestLimitError({ resource, maximumBytes: MAX_MEDIA_INGEST_BYTES })
    const whole = yield* readFile(files, input, resource)
    return {
      type: "file" as const,
      uri: virtual ? resource : pathToFileURL(input).href,
      name: virtual ? path.posix.basename(input) : path.basename(input),
      content: Buffer.from(whole.bytes).toString("base64"),
      encoding: "base64" as const,
      mime: media,
    }
  }

  const paged = first.info.size > MAX_READ_BYTES || page.offset !== undefined || page.limit !== undefined
  if (!paged) {
    if (first.bytes.includes(0)) return yield* new BinaryFileError({ resource })
    return {
      type: "file" as const,
      uri: virtual ? resource : pathToFileURL(input).href,
      name: virtual ? path.posix.basename(input) : path.basename(input),
      content: new TextDecoder().decode(first.bytes).split("\n").map(clampLine).join("\n"),
      encoding: "utf8" as const,
      mime: mimeType(input),
    }
  }

  if (first.bytes.length >= first.info.size) return yield* completePage(input, resource, first.bytes, page)
  return yield* streamPage(files, input, resource, first, page)
})

// Streams a file larger than the first chunk, keeping only what the page can still need: the chunks from the start of
// the requested line on, or the file's last lines while the page may turn out to be its tail.
const streamPage = Effect.fnUntraced(function* (
  files: Files,
  input: AbsolutePath,
  resource: string,
  first: { readonly bytes: Uint8Array; readonly info: { readonly size: number } },
  page: PageInput,
): Effect.fn.Return<TextPage, ReadError> {
  const offset = page.offset || 1
  const limit = Math.min(page.limit || MAX_READ_LINES, MAX_READ_LINES)
  // Lines counted back from the end that the window keeps: a negative offset's start, or the tail returned for an
  // offset past the end.
  const tail = offset < 0 ? Math.min(-offset, MAX_READ_LINES) : Math.min(page.limit || TAIL_LINES, TAIL_LINES)
  const window: Chunk[] = []
  // What the window no longer holds: the newlines of the chunks it dropped, whether there were any, and whether they
  // had a NUL byte.
  let before = 0
  let dropped = false
  let binaryBefore = false
  // Whether the window holds the start of the line after the file's `newline`th newline.
  const holds = (newline: number) => newline > before || (newline === 0 && !dropped)
  let size = 0
  let lines = 0
  let run = 0
  let read = 0
  let last: number | undefined
  let ended = false
  let next = first.bytes
  while (true) {
    if (next.length === 0) ended = true
    else {
      const item = clip(next, run)
      read += next.length
      last = next.at(-1)
      lines += item.lines
      run = item.run
      if (item.bytes.length > 0) {
        window.push(item)
        size += item.bytes.length
      }
      // Drop the oldest chunk while it ends before the requested line and the rest still hold the tail lines.
      while (
        window.length > 1 &&
        (offset < 0 || before + window[0].lines < offset - 1) &&
        lines - before - window[0].lines > tail
      ) {
        const oldest = window.shift()!
        before += oldest.lines
        size -= oldest.bytes.length
        dropped = true
        binaryBefore ||= oldest.bytes.includes(0)
      }
    }

    const eof = ended || read >= first.info.size
    const total = lineCount(lines, last)
    if (eof && offset > total) {
      const skipped = Math.max(0, total - tail)
      if (!holds(skipped)) return yield* Effect.die("Read window dropped the tail")
      // Decode from the newline ending the last skipped line, so a byte order mark
      // starting the tail stays content. That empty first line is never in the tail.
      const result = textPage(
        windowSlice(window, Math.max(0, windowOffset(window, skipped - before) - 1), size),
        true,
        page,
      )
      if (result === undefined) return yield* Effect.die("Read page did not settle for a complete file")
      return yield* makeTextPage(
        input,
        resource,
        { ...result, offset: Math.max(0, skipped - 1) + result.offset },
        binaryBefore || window.some((item) => item.bytes.includes(0)),
      )
    }
    if (eof || (offset > 0 && lines >= offset - 1)) {
      // A negative offset only resolves at EOF, counting back from the last line.
      const from = offset < 0 ? Math.max(1, total + offset + 1) : offset
      // A negative offset reaching further back than the window reads from the start like a positive one.
      if (!holds(from - 1)) return yield* streamPage(files, input, resource, first, { ...page, offset: from })
      const start = windowOffset(window, from - 1 - before)
      // Decode only the requested lines plus one byte, enough to tell whether more follow.
      const end = Math.min(size, windowOffset(window, from - 1 - before + limit) + 1)
      const result = textPage(windowSlice(window, start, end), eof && end === size, { limit })
      if (result !== undefined) {
        const translated = {
          ...result,
          offset: from,
          ...(result.next === undefined ? { next: undefined } : { next: from + result.next - 1 }),
        }
        const binary = binaryBefore || windowHasNul(window, start + result.consumed)
        return yield* makeTextPage(input, resource, translated, binary)
      }
    }

    next = (yield* readFile(files, input, resource, { offset: read, length: FIRST_CHUNK })).bytes
  }
})

const readFile = (
  files: Files,
  input: AbsolutePath,
  resource: string,
  range?: { readonly offset: number; readonly length: number },
) =>
  files
    .read(input, range)
    .pipe(
      Effect.catchTag("Environment.WrongKind", () => Effect.fail(new PathKindError({ resource, expected: "a file" }))),
    )

const completePage = Effect.fnUntraced(function* (
  input: AbsolutePath,
  resource: string,
  bytes: Uint8Array,
  page: PageInput,
) {
  const total = lineCount(countNewlines(bytes), bytes.at(-1))
  const result = textPage(
    bytes,
    true,
    page.offset !== undefined && page.offset < 0 ? { ...page, offset: Math.max(1, total + page.offset + 1) } : page,
  )
  if (result === undefined) return yield* Effect.die("Read page did not settle for a complete file")
  return yield* makeTextPage(input, resource, result, bytes.subarray(0, result.consumed).includes(0))
})

const makeTextPage = Effect.fnUntraced(function* (
  input: AbsolutePath,
  resource: string,
  result: NonNullable<ReturnType<typeof textPage>>,
  binary: boolean,
) {
  if (binary) return yield* new BinaryFileError({ resource })
  return new TextPage({
    type: "text-page",
    content: result.entries.join("\n"),
    mime: mimeType(input),
    offset: result.offset,
    truncated: result.next !== undefined,
    ...(result.next === undefined ? {} : { next: result.next }),
  })
})

const list = (items: ReadonlyArray<Environment.DirEntry>, page: PageInput, sep = path.sep) => {
  const offset = page.offset || 1
  const limit = Math.min(page.limit || MAX_READ_LINES, MAX_READ_LINES)
  const visible = items
    .flatMap((item) =>
      item.type === "other"
        ? []
        : [
            ListEntry.make({
              path: RelativePath.make(item.name + (item.type === "directory" ? sep : "")),
              type: item.type,
            }),
          ],
    )
    .sort((a, b) =>
      a.type === "directory"
        ? b.type === "directory"
          ? a.path.localeCompare(b.path)
          : -1
        : b.type === "directory"
          ? 1
          : a.path.localeCompare(b.path),
    )
  const selected = visible.slice(offset - 1, offset - 1 + limit)
  const truncated = offset - 1 + selected.length < visible.length
  return new ListPage({
    type: "list-page",
    entries: selected,
    truncated,
    ...(truncated ? { next: offset + selected.length } : {}),
  })
}

const textPage = (bytes: Uint8Array, eof: boolean, page: PageInput) => {
  const offset = page.offset || 1
  const limit = Math.min(page.limit || MAX_READ_LINES, MAX_READ_LINES)
  const decoded = new TextDecoder().decode(bytes)
  const split = decoded.split("\n")
  const complete = eof ? (split.at(-1) === "" ? split.slice(0, -1) : split) : split.slice(0, -1)
  const available = complete.map((line) => (line.endsWith("\r") ? line.slice(0, -1) : line))

  const entries: string[] = []
  let size = 0
  // An offset past the end returns the file's last lines that fit, ending at the last line.
  if (eof && offset > Math.max(1, available.length)) {
    for (const value of available.slice(-Math.min(page.limit || TAIL_LINES, TAIL_LINES)).toReversed()) {
      const text = clampLine(value)
      size += Buffer.byteLength(text, "utf-8") + (entries.length > 0 ? 1 : 0)
      if (size > MAX_READ_BYTES) break
      entries.unshift(text)
    }
    return { entries, offset: available.length - entries.length + 1, next: undefined, consumed: bytes.length }
  }
  let next: number | undefined
  for (const [index, value] of available.slice(offset - 1).entries()) {
    const line = offset + index
    if (entries.length >= limit || size >= MAX_READ_BYTES) {
      next = line
      break
    }
    const text = clampLine(value)
    const lineSize = Buffer.byteLength(text, "utf-8") + (entries.length > 0 ? 1 : 0)
    if (size + lineSize > MAX_READ_BYTES) {
      next = line
      break
    }
    entries.push(text)
    size += lineSize
  }
  if (next === undefined && entries.length >= limit && (!eof || offset - 1 + entries.length < available.length))
    next = offset + entries.length
  if (!eof && next === undefined) return

  const consumedLines = next === undefined ? available.length : next - 1
  const consumed = consumedLines === 0 ? 0 : (nthNewline(bytes, consumedLines) ?? bytes.length)
  return { entries, offset, next, consumed }
}

const clampLine = (line: string) =>
  line.length > MAX_LINE_LENGTH ? line.slice(0, MAX_LINE_LENGTH) + MAX_LINE_SUFFIX : line

// A trailing newline ends the last line instead of starting an empty one.
const lineCount = (newlines: number, last: number | undefined) => newlines + (last === undefined || last === 10 ? 0 : 1)

type Chunk = { readonly bytes: Uint8Array; readonly lines: number }

// A line is shown cut to MAX_LINE_LENGTH UTF-16 code units, none of which takes more than 3 bytes of UTF-8. Keeping 4
// bytes for each still shows a longer line exactly, with room for a character cut at the end.
const LINE_BYTES = MAX_LINE_LENGTH * 4
const NUL = new Uint8Array([0])

/**
 * Counts the newlines in `bytes` and keeps at most LINE_BYTES of each line, given `run` bytes of its first line kept
 * already. A NUL byte in a cut part is kept as one, so the binary check still sees it.
 */
const clip = (bytes: Uint8Array, run: number) => {
  const kept: Uint8Array[] = []
  let lines = 0
  let copied = 0
  let start = 0
  let length = run
  while (true) {
    const newline = bytes.indexOf(10, start)
    const end = newline === -1 ? bytes.length : newline
    const room = Math.max(0, LINE_BYTES - length)
    if (end - start > room) {
      kept.push(bytes.subarray(copied, start + room))
      if (bytes.subarray(start + room, end).includes(0)) kept.push(NUL)
      copied = end
    }
    if (newline === -1)
      return {
        bytes: kept.length === 0 ? bytes : Buffer.concat([...kept, bytes.subarray(copied)]),
        lines,
        run: length + Math.min(end - start, room),
      }
    lines++
    length = 0
    start = newline + 1
  }
}

const countNewlines = (bytes: Uint8Array) => {
  let count = 0
  for (let index = bytes.indexOf(10); index !== -1; index = bytes.indexOf(10, index + 1)) count++
  return count
}

// Byte offset in the window just past its `newline`th newline, or its end when it has fewer.
const windowOffset = (window: ReadonlyArray<Chunk>, newline: number) => {
  let remaining = newline
  let offset = 0
  for (const item of window) {
    if (remaining <= 0) return offset
    if (remaining <= item.lines) return offset + (nthNewline(item.bytes, remaining) ?? item.bytes.length)
    remaining -= item.lines
    offset += item.bytes.length
  }
  return offset
}

const windowSlice = (window: ReadonlyArray<Chunk>, start: number, end: number) => {
  let position = 0
  return Buffer.concat(
    window.flatMap((item) => {
      const itemStart = position
      position += item.bytes.length
      if (position <= start || itemStart >= end) return []
      return [item.bytes.subarray(Math.max(0, start - itemStart), end - itemStart)]
    }),
  )
}

// Whether the window's first `end` bytes include a NUL byte.
const windowHasNul = (window: ReadonlyArray<Chunk>, end: number) => {
  let position = 0
  return window.some((item) => {
    const found = position < end && item.bytes.subarray(0, end - position).includes(0)
    position += item.bytes.length
    return found
  })
}

const nthNewline = (bytes: Uint8Array, count: number) => {
  let found = 0
  for (let index = bytes.indexOf(10); index !== -1; index = bytes.indexOf(10, index + 1)) {
    found++
    if (found === count) return index + 1
  }
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const environment = yield* Environment.Service
    return Service.of({
      list: environment.files.list,
      read: (path, resource, page) => read(environment.files, path, resource, page),
    })
  }),
)

export const node = makeLocationNode({ service: Service, layer, deps: [Environment.node] })
