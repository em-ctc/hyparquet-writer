"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/index.js
var index_exports = {};
__export(index_exports, {
  ByteWriter: () => ByteWriter,
  ParquetWriter: () => ParquetWriter,
  autoSchemaElement: () => autoSchemaElement,
  parquetWrite: () => parquetWrite,
  parquetWriteBuffer: () => parquetWriteBuffer,
  schemaFromColumnData: () => schemaFromColumnData
});
module.exports = __toCommonJS(index_exports);

// src/bytewriter.js
function ByteWriter() {
  this.buffer = new ArrayBuffer(1024);
  this.view = new DataView(this.buffer);
  this.offset = 0;
  this.index = 0;
  return this;
}
ByteWriter.prototype.ensure = function(size) {
  if (this.index + size > this.buffer.byteLength) {
    const newSize = Math.max(this.buffer.byteLength * 2, this.index + size);
    const newBuffer = new ArrayBuffer(newSize);
    new Uint8Array(newBuffer).set(new Uint8Array(this.buffer));
    this.buffer = newBuffer;
    this.view = new DataView(this.buffer);
  }
};
ByteWriter.prototype.finish = function() {
};
ByteWriter.prototype.getBuffer = function() {
  return this.buffer.slice(0, this.index);
};
ByteWriter.prototype.appendUint8 = function(value) {
  this.ensure(this.index + 1);
  this.view.setUint8(this.index, value);
  this.offset++;
  this.index++;
};
ByteWriter.prototype.appendUint32 = function(value) {
  this.ensure(this.index + 4);
  this.view.setUint32(this.index, value, true);
  this.offset += 4;
  this.index += 4;
};
ByteWriter.prototype.appendInt32 = function(value) {
  this.ensure(this.index + 4);
  this.view.setInt32(this.index, value, true);
  this.offset += 4;
  this.index += 4;
};
ByteWriter.prototype.appendInt64 = function(value) {
  this.ensure(this.index + 8);
  this.view.setBigInt64(this.index, BigInt(value), true);
  this.offset += 8;
  this.index += 8;
};
ByteWriter.prototype.appendFloat32 = function(value) {
  this.ensure(this.index + 8);
  this.view.setFloat32(this.index, value, true);
  this.offset += 4;
  this.index += 4;
};
ByteWriter.prototype.appendFloat64 = function(value) {
  this.ensure(this.index + 8);
  this.view.setFloat64(this.index, value, true);
  this.offset += 8;
  this.index += 8;
};
ByteWriter.prototype.appendBuffer = function(value) {
  this.appendBytes(new Uint8Array(value));
};
ByteWriter.prototype.appendBytes = function(value) {
  this.ensure(this.index + value.length);
  new Uint8Array(this.buffer, this.index, value.length).set(value);
  this.offset += value.length;
  this.index += value.length;
};
ByteWriter.prototype.appendVarInt = function(value) {
  while (true) {
    if ((value & ~127) === 0) {
      this.appendUint8(value);
      return;
    } else {
      this.appendUint8(value & 127 | 128);
      value >>>= 7;
    }
  }
};
ByteWriter.prototype.appendVarBigInt = function(value) {
  while (true) {
    if ((value & ~0x7fn) === 0n) {
      this.appendUint8(Number(value));
      return;
    } else {
      this.appendUint8(Number(value & 0x7fn | 0x80n));
      value >>= 7n;
    }
  }
};

// src/unconvert.js
var dayMillis = 864e5;
function unconvert(element, values) {
  const { type, converted_type: ctype, logical_type: ltype } = element;
  if (ctype === "DECIMAL") {
    const factor = 10 ** (element.scale || 0);
    return values.map((v) => {
      if (v === null || v === void 0) return v;
      if (typeof v !== "number") throw new Error("DECIMAL must be a number");
      return unconvertDecimal(element, BigInt(Math.round(v * factor)));
    });
  }
  if (ctype === "DATE") {
    return Array.from(values).map((v) => v && v.getTime() / dayMillis);
  }
  if (ctype === "TIMESTAMP_MILLIS") {
    return Array.from(values).map((v) => v && BigInt(v.getTime()));
  }
  if (ctype === "TIMESTAMP_MICROS") {
    return Array.from(values).map((v) => v && BigInt(v.getTime() * 1e3));
  }
  if (ctype === "JSON") {
    if (!Array.isArray(values)) throw new Error("JSON must be an array");
    const encoder = new TextEncoder();
    return values.map((v) => encoder.encode(JSON.stringify(v)));
  }
  if (ctype === "UTF8") {
    if (!Array.isArray(values)) throw new Error("strings must be an array");
    const encoder = new TextEncoder();
    return values.map((v) => typeof v === "string" ? encoder.encode(v) : v);
  }
  if (ltype?.type === "FLOAT16") {
    if (type !== "FIXED_LEN_BYTE_ARRAY") throw new Error("FLOAT16 must be FIXED_LEN_BYTE_ARRAY type");
    if (element.type_length !== 2) throw new Error("FLOAT16 expected type_length to be 2 bytes");
    return Array.from(values).map(unconvertFloat16);
  }
  if (ltype?.type === "UUID") {
    if (!Array.isArray(values)) throw new Error("UUID must be an array");
    if (type !== "FIXED_LEN_BYTE_ARRAY") throw new Error("UUID must be FIXED_LEN_BYTE_ARRAY type");
    if (element.type_length !== 16) throw new Error("UUID expected type_length to be 16 bytes");
    return values.map(unconvertUuid);
  }
  return values;
}
function unconvertUuid(value) {
  if (value === void 0 || value === null) return;
  if (value instanceof Uint8Array) return value;
  if (typeof value === "string") {
    const uuidRegex = /^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$/i;
    if (!uuidRegex.test(value)) {
      throw new Error("UUID must be a valid UUID string");
    }
    value = value.replace(/-/g, "").toLowerCase();
    const bytes = new Uint8Array(16);
    for (let i = 0; i < 16; i++) {
      bytes[i] = parseInt(value.slice(i * 2, i * 2 + 2), 16);
    }
    return bytes;
  }
  throw new Error("UUID must be a string or Uint8Array");
}
function unconvertMinMax(value, element) {
  if (value === void 0 || value === null) return void 0;
  const { type, converted_type } = element;
  if (type === "BOOLEAN") return new Uint8Array([value ? 1 : 0]);
  if (converted_type === "DECIMAL") {
    if (typeof value !== "number") throw new Error("DECIMAL must be a number");
    const factor = 10 ** (element.scale || 0);
    const out = unconvertDecimal(element, BigInt(Math.round(value * factor)));
    if (out instanceof Uint8Array) return out;
    if (typeof out === "number") {
      const buffer = new ArrayBuffer(4);
      new DataView(buffer).setFloat32(0, out, true);
      return new Uint8Array(buffer);
    }
    if (typeof out === "bigint") {
      const buffer = new ArrayBuffer(8);
      new DataView(buffer).setBigInt64(0, out, true);
      return new Uint8Array(buffer);
    }
  }
  if (type === "BYTE_ARRAY" || type === "FIXED_LEN_BYTE_ARRAY") {
    if (value instanceof Uint8Array) return value.slice(0, 16);
    return new TextEncoder().encode(value.toString().slice(0, 16));
  }
  if (type === "FLOAT" && typeof value === "number") {
    const buffer = new ArrayBuffer(4);
    new DataView(buffer).setFloat32(0, value, true);
    return new Uint8Array(buffer);
  }
  if (type === "DOUBLE" && typeof value === "number") {
    const buffer = new ArrayBuffer(8);
    new DataView(buffer).setFloat64(0, value, true);
    return new Uint8Array(buffer);
  }
  if (type === "INT32" && typeof value === "number") {
    const buffer = new ArrayBuffer(4);
    new DataView(buffer).setInt32(0, value, true);
    return new Uint8Array(buffer);
  }
  if (type === "INT64" && typeof value === "bigint") {
    const buffer = new ArrayBuffer(8);
    new DataView(buffer).setBigInt64(0, value, true);
    return new Uint8Array(buffer);
  }
  if (type === "INT32" && converted_type === "DATE" && value instanceof Date) {
    const buffer = new ArrayBuffer(4);
    new DataView(buffer).setInt32(0, Math.floor(value.getTime() / dayMillis), true);
    return new Uint8Array(buffer);
  }
  if (type === "INT64" && converted_type === "TIMESTAMP_MILLIS" && value instanceof Date) {
    const buffer = new ArrayBuffer(8);
    new DataView(buffer).setBigInt64(0, BigInt(value.getTime()), true);
    return new Uint8Array(buffer);
  }
  throw new Error(`unsupported type for statistics: ${type} with value ${value}`);
}
function unconvertStatistics(stats, element) {
  return {
    field_1: unconvertMinMax(stats.max, element),
    field_2: unconvertMinMax(stats.min, element),
    field_3: stats.null_count,
    field_4: stats.distinct_count,
    field_5: unconvertMinMax(stats.max_value, element),
    field_6: unconvertMinMax(stats.min_value, element),
    field_7: stats.is_max_value_exact,
    field_8: stats.is_min_value_exact
  };
}
function unconvertDecimal({ type, type_length }, value) {
  if (type === "INT32") return Number(value);
  if (type === "INT64") return value;
  if (type === "FIXED_LEN_BYTE_ARRAY" && !type_length) {
    throw new Error("fixed length byte array type_length is required");
  }
  if (!type_length && !value) return new Uint8Array();
  const bytes = [];
  while (true) {
    const byte = Number(value & 0xffn);
    bytes.unshift(byte);
    value >>= 8n;
    if (type_length) {
      if (bytes.length >= type_length) break;
    } else {
      const sign = byte & 128;
      if (!sign && value === 0n || sign && value === -1n) {
        break;
      }
    }
  }
  return new Uint8Array(bytes);
}
function unconvertFloat16(value) {
  if (value === void 0 || value === null) return;
  if (typeof value !== "number") throw new Error("parquet float16 expected number value");
  if (Number.isNaN(value)) return new Uint8Array([0, 126]);
  const sign = value < 0 || Object.is(value, -0) ? 1 : 0;
  const abs = Math.abs(value);
  if (!isFinite(abs)) return new Uint8Array([0, sign << 7 | 124]);
  if (abs === 0) return new Uint8Array([0, sign << 7]);
  const buf = new ArrayBuffer(4);
  new Float32Array(buf)[0] = abs;
  const bits32 = new Uint32Array(buf)[0];
  let exp32 = bits32 >>> 23 & 255;
  let mant32 = bits32 & 8388607;
  exp32 -= 127;
  if (exp32 < -14) {
    const shift = -14 - exp32;
    mant32 = (mant32 | 8388608) >> shift + 13;
    if (mant32 & 1) mant32 += 1;
    const bits162 = sign << 15 | mant32;
    return new Uint8Array([bits162 & 255, bits162 >> 8]);
  }
  if (exp32 > 15) return new Uint8Array([0, sign << 7 | 124]);
  let exp16 = exp32 + 15;
  mant32 = mant32 + 4096;
  if (mant32 & 8388608) {
    mant32 = 0;
    if (++exp16 === 31)
      return new Uint8Array([0, sign << 7 | 124]);
  }
  const bits16 = sign << 15 | exp16 << 10 | mant32 >> 13;
  return new Uint8Array([bits16 & 255, bits16 >> 8]);
}

// src/plain.js
function writePlain(writer, values, type, fixedLength) {
  if (type === "BOOLEAN") {
    writePlainBoolean(writer, values);
  } else if (type === "INT32") {
    writePlainInt32(writer, values);
  } else if (type === "INT64") {
    writePlainInt64(writer, values);
  } else if (type === "FLOAT") {
    writePlainFloat(writer, values);
  } else if (type === "DOUBLE") {
    writePlainDouble(writer, values);
  } else if (type === "BYTE_ARRAY") {
    writePlainByteArray(writer, values);
  } else if (type === "FIXED_LEN_BYTE_ARRAY") {
    if (!fixedLength) throw new Error("parquet FIXED_LEN_BYTE_ARRAY expected type_length");
    writePlainByteArrayFixed(writer, values, fixedLength);
  } else {
    throw new Error(`parquet unsupported type: ${type}`);
  }
}
function writePlainBoolean(writer, values) {
  let currentByte = 0;
  for (let i = 0; i < values.length; i++) {
    if (typeof values[i] !== "boolean") throw new Error("parquet expected boolean value");
    const bitOffset = i % 8;
    if (values[i]) {
      currentByte |= 1 << bitOffset;
    }
    if (bitOffset === 7) {
      writer.appendUint8(currentByte);
      currentByte = 0;
    }
  }
  if (values.length % 8 !== 0) {
    writer.appendUint8(currentByte);
  }
}
function writePlainInt32(writer, values) {
  for (const value of values) {
    if (!Number.isSafeInteger(value)) throw new Error("parquet expected integer value");
    writer.appendInt32(value);
  }
}
function writePlainInt64(writer, values) {
  for (const value of values) {
    if (typeof value !== "bigint") throw new Error("parquet expected bigint value");
    writer.appendInt64(value);
  }
}
function writePlainFloat(writer, values) {
  for (const value of values) {
    if (typeof value !== "number") throw new Error("parquet expected number value");
    writer.appendFloat32(value);
  }
}
function writePlainDouble(writer, values) {
  for (const value of values) {
    if (typeof value !== "number") throw new Error("parquet expected number value");
    writer.appendFloat64(value);
  }
}
function writePlainByteArray(writer, values) {
  for (const value of values) {
    let bytes = value;
    if (typeof bytes === "string") {
      bytes = new TextEncoder().encode(value);
    }
    if (!(bytes instanceof Uint8Array)) {
      throw new Error("parquet expected Uint8Array value");
    }
    writer.appendUint32(bytes.length);
    writer.appendBytes(bytes);
  }
}
function writePlainByteArrayFixed(writer, values, fixedLength) {
  for (const value of values) {
    if (!(value instanceof Uint8Array)) throw new Error("parquet expected Uint8Array value");
    if (value.length !== fixedLength) throw new Error(`parquet expected Uint8Array of length ${fixedLength}`);
    writer.appendBytes(value);
  }
}

// src/snappy.js
var BLOCK_LOG = 16;
var BLOCK_SIZE = 1 << BLOCK_LOG;
var MAX_HASH_TABLE_BITS = 14;
var globalHashTables = new Array(MAX_HASH_TABLE_BITS + 1);
function snappyCompress(writer, input) {
  writer.appendVarInt(input.length);
  if (input.length === 0) return;
  let pos = 0;
  while (pos < input.length) {
    const fragmentSize = Math.min(input.length - pos, BLOCK_SIZE);
    compressFragment(input, pos, fragmentSize, writer);
    pos += fragmentSize;
  }
}
function hashFunc(key, hashFuncShift) {
  return key * 506832829 >>> hashFuncShift;
}
function load32(array, pos) {
  return array[pos] + (array[pos + 1] << 8) + (array[pos + 2] << 16) + (array[pos + 3] << 24);
}
function equals32(array, pos1, pos2) {
  return array[pos1] === array[pos2] && array[pos1 + 1] === array[pos2 + 1] && array[pos1 + 2] === array[pos2 + 2] && array[pos1 + 3] === array[pos2 + 3];
}
function emitLiteral(input, ip, len, writer) {
  if (len <= 60) {
    writer.appendUint8(len - 1 << 2);
  } else if (len < 256) {
    writer.appendUint8(60 << 2);
    writer.appendUint8(len - 1);
  } else {
    writer.appendUint8(61 << 2);
    writer.appendUint8(len - 1 & 255);
    writer.appendUint8(len - 1 >>> 8);
  }
  writer.appendBytes(input.subarray(ip, ip + len));
}
function emitCopyLessThan64(writer, offset, len) {
  if (len < 12 && offset < 2048) {
    writer.appendUint8(1 + (len - 4 << 2) + (offset >>> 8 << 5));
    writer.appendUint8(offset & 255);
  } else {
    writer.appendUint8(2 + (len - 1 << 2));
    writer.appendUint8(offset & 255);
    writer.appendUint8(offset >>> 8);
  }
}
function emitCopy(writer, offset, len) {
  while (len >= 68) {
    emitCopyLessThan64(writer, offset, 64);
    len -= 64;
  }
  if (len > 64) {
    emitCopyLessThan64(writer, offset, 60);
    len -= 60;
  }
  emitCopyLessThan64(writer, offset, len);
}
function compressFragment(input, ip, inputSize, writer) {
  let hashTableBits = 1;
  while (1 << hashTableBits <= inputSize && hashTableBits <= MAX_HASH_TABLE_BITS) {
    hashTableBits += 1;
  }
  hashTableBits -= 1;
  const hashFuncShift = 32 - hashTableBits;
  if (typeof globalHashTables[hashTableBits] === "undefined") {
    globalHashTables[hashTableBits] = new Uint16Array(1 << hashTableBits);
  }
  const hashTable = globalHashTables[hashTableBits];
  for (let i = 0; i < hashTable.length; i++) {
    hashTable[i] = 0;
  }
  const ipEnd = ip + inputSize;
  let ipLimit;
  const baseIp = ip;
  let nextEmit = ip;
  let hash, nextHash;
  let nextIp, candidate, skip;
  let bytesBetweenHashLookups;
  let base, matched, offset;
  let prevHash, curHash;
  let flag = true;
  const INPUT_MARGIN = 15;
  if (inputSize >= INPUT_MARGIN) {
    ipLimit = ipEnd - INPUT_MARGIN;
    ip += 1;
    nextHash = hashFunc(load32(input, ip), hashFuncShift);
    while (flag) {
      skip = 32;
      nextIp = ip;
      do {
        ip = nextIp;
        hash = nextHash;
        bytesBetweenHashLookups = skip >>> 5;
        skip += 1;
        nextIp = ip + bytesBetweenHashLookups;
        if (ip > ipLimit) {
          flag = false;
          break;
        }
        nextHash = hashFunc(load32(input, nextIp), hashFuncShift);
        candidate = baseIp + hashTable[hash];
        hashTable[hash] = ip - baseIp;
      } while (!equals32(input, ip, candidate));
      if (!flag) {
        break;
      }
      emitLiteral(input, nextEmit, ip - nextEmit, writer);
      do {
        base = ip;
        matched = 4;
        while (ip + matched < ipEnd && input[ip + matched] === input[candidate + matched]) {
          matched++;
        }
        ip += matched;
        offset = base - candidate;
        emitCopy(writer, offset, matched);
        nextEmit = ip;
        if (ip >= ipLimit) {
          flag = false;
          break;
        }
        prevHash = hashFunc(load32(input, ip - 1), hashFuncShift);
        hashTable[prevHash] = ip - 1 - baseIp;
        curHash = hashFunc(load32(input, ip), hashFuncShift);
        candidate = baseIp + hashTable[curHash];
        hashTable[curHash] = ip - baseIp;
      } while (equals32(input, ip, candidate));
      if (!flag) {
        break;
      }
      ip += 1;
      nextHash = hashFunc(load32(input, ip), hashFuncShift);
    }
  }
  if (nextEmit < ipEnd) {
    emitLiteral(input, nextEmit, ipEnd - nextEmit, writer);
  }
}

// node_modules/hyparquet/src/constants.js
var ParquetType = [
  "BOOLEAN",
  "INT32",
  "INT64",
  "INT96",
  // deprecated
  "FLOAT",
  "DOUBLE",
  "BYTE_ARRAY",
  "FIXED_LEN_BYTE_ARRAY"
];
var Encoding = [
  "PLAIN",
  "GROUP_VAR_INT",
  // deprecated
  "PLAIN_DICTIONARY",
  "RLE",
  "BIT_PACKED",
  // deprecated
  "DELTA_BINARY_PACKED",
  "DELTA_LENGTH_BYTE_ARRAY",
  "DELTA_BYTE_ARRAY",
  "RLE_DICTIONARY",
  "BYTE_STREAM_SPLIT"
];
var FieldRepetitionType = [
  "REQUIRED",
  "OPTIONAL",
  "REPEATED"
];
var ConvertedType = [
  "UTF8",
  "MAP",
  "MAP_KEY_VALUE",
  "LIST",
  "ENUM",
  "DECIMAL",
  "DATE",
  "TIME_MILLIS",
  "TIME_MICROS",
  "TIMESTAMP_MILLIS",
  "TIMESTAMP_MICROS",
  "UINT_8",
  "UINT_16",
  "UINT_32",
  "UINT_64",
  "INT_8",
  "INT_16",
  "INT_32",
  "INT_64",
  "JSON",
  "BSON",
  "INTERVAL"
];
var CompressionCodec = [
  "UNCOMPRESSED",
  "SNAPPY",
  "GZIP",
  "LZO",
  "BROTLI",
  "LZ4",
  "ZSTD",
  "LZ4_RAW"
];
var PageType = [
  "DATA_PAGE",
  "INDEX_PAGE",
  "DICTIONARY_PAGE",
  "DATA_PAGE_V2"
];

// src/encoding.js
function writeRleBitPackedHybrid(writer, values, bitWidth) {
  const offsetStart = writer.offset;
  const rle = new ByteWriter();
  writeRle(rle, values, bitWidth);
  const bitPacked = new ByteWriter();
  writeBitPacked(bitPacked, values, bitWidth);
  if (rle.offset < bitPacked.offset) {
    writer.appendBuffer(rle.getBuffer());
  } else {
    writer.appendBuffer(bitPacked.getBuffer());
  }
  return writer.offset - offsetStart;
}
function writeBitPacked(writer, values, bitWidth) {
  const numGroups = Math.ceil(values.length / 8);
  const header = numGroups << 1 | 1;
  writer.appendVarInt(header);
  if (bitWidth === 0 || values.length === 0) {
    return;
  }
  const mask = (1 << bitWidth) - 1;
  let buffer = 0;
  let bitsUsed = 0;
  for (let i = 0; i < values.length; i++) {
    const v = values[i] & mask;
    buffer |= v << bitsUsed;
    bitsUsed += bitWidth;
    while (bitsUsed >= 8) {
      writer.appendUint8(buffer & 255);
      buffer >>>= 8;
      bitsUsed -= 8;
    }
  }
  const totalNeeded = numGroups * 8;
  for (let padCount = values.length; padCount < totalNeeded; padCount++) {
    buffer |= 0 << bitsUsed;
    bitsUsed += bitWidth;
    while (bitsUsed >= 8) {
      writer.appendUint8(buffer & 255);
      buffer >>>= 8;
      bitsUsed -= 8;
    }
  }
  if (bitsUsed > 0) {
    writer.appendUint8(buffer & 255);
  }
}
function writeRle(writer, values, bitWidth) {
  if (!values.length) return;
  let currentValue = values[0];
  let count = 1;
  for (let i = 1; i <= values.length; i++) {
    if (i < values.length && values[i] === currentValue) {
      count++;
    } else {
      const header = count << 1;
      writer.appendVarInt(header);
      const width = bitWidth + 7 >> 3;
      for (let j = 0; j < width; j++) {
        writer.appendUint8(currentValue >> (j << 3) & 255);
      }
      if (i < values.length) {
        currentValue = values[i];
        count = 1;
      }
    }
  }
}

// node_modules/hyparquet/src/thrift.js
var CompactType = {
  STOP: 0,
  TRUE: 1,
  FALSE: 2,
  BYTE: 3,
  I16: 4,
  I32: 5,
  I64: 6,
  DOUBLE: 7,
  BINARY: 8,
  LIST: 9,
  SET: 10,
  MAP: 11,
  STRUCT: 12,
  UUID: 13
};

// src/thrift.js
function serializeTCompactProtocol(writer, data) {
  let lastFid = 0;
  for (const [key, value] of Object.entries(data)) {
    if (value === void 0) continue;
    const fid = parseInt(key.replace(/^field_/, ""), 10);
    if (Number.isNaN(fid)) {
      throw new Error(`thrift invalid field name: ${key}. Expected "field_###".`);
    }
    const type = getCompactTypeForValue(value);
    const delta = fid - lastFid;
    if (delta <= 0) {
      throw new Error(`thrift non-monotonic field ID: fid=${fid}, lastFid=${lastFid}`);
    }
    writer.appendUint8(delta << 4 | type);
    writeElement(writer, type, value);
    lastFid = fid;
  }
  writer.appendUint8(CompactType.STOP);
}
function getCompactTypeForValue(value) {
  if (value === true) return CompactType.TRUE;
  if (value === false) return CompactType.FALSE;
  if (Number.isInteger(value)) return CompactType.I32;
  if (typeof value === "number") return CompactType.DOUBLE;
  if (typeof value === "bigint") return CompactType.I64;
  if (typeof value === "string") return CompactType.BINARY;
  if (value instanceof Uint8Array) return CompactType.BINARY;
  if (Array.isArray(value)) return CompactType.LIST;
  if (value && typeof value === "object") return CompactType.STRUCT;
  throw new Error(`Cannot determine thrift compact type for: ${value}`);
}
function writeElement(writer, type, value) {
  if (type === CompactType.TRUE) return;
  if (type === CompactType.FALSE) return;
  if (type === CompactType.BYTE && typeof value === "number") {
    writer.appendUint8(value);
  } else if (type === CompactType.I32 && typeof value === "number") {
    const zigzag = value << 1 ^ value >> 31;
    writer.appendVarInt(zigzag);
  } else if (type === CompactType.I64 && typeof value === "bigint") {
    const zigzag = value << 1n ^ value >> 63n;
    writer.appendVarBigInt(zigzag);
  } else if (type === CompactType.DOUBLE && typeof value === "number") {
    writer.appendFloat64(value);
  } else if (type === CompactType.BINARY && typeof value === "string") {
    const bytes = new TextEncoder().encode(value);
    writer.appendVarInt(bytes.length);
    writer.appendBytes(bytes);
  } else if (type === CompactType.BINARY && value instanceof Uint8Array) {
    writer.appendVarInt(value.byteLength);
    writer.appendBytes(value);
  } else if (type === CompactType.LIST && Array.isArray(value)) {
    const size = value.length;
    if (size === 0) {
      writer.appendUint8(0 << 4 | CompactType.BYTE);
      return;
    }
    const elemType = getCompactTypeForValue(value[0]);
    const sizeNibble = size > 14 ? 15 : size;
    writer.appendUint8(sizeNibble << 4 | elemType);
    if (size > 14) {
      writer.appendVarInt(size);
    }
    if (elemType === CompactType.TRUE || elemType === CompactType.FALSE) {
      for (const v of value) {
        writer.appendUint8(v ? 1 : 0);
      }
    } else {
      for (const v of value) {
        writeElement(writer, elemType, v);
      }
    }
  } else if (type === CompactType.STRUCT && typeof value === "object") {
    let lastFid = 0;
    for (const [k, v] of Object.entries(value)) {
      if (v === void 0) continue;
      const fid = parseInt(k.replace(/^field_/, ""), 10);
      if (Number.isNaN(fid)) {
        throw new Error(`Invalid sub-field name: ${k}. Expected "field_###"`);
      }
      const t = getCompactTypeForValue(v);
      const delta = fid - lastFid;
      if (delta <= 0) {
        throw new Error(`Non-monotonic fid in struct: fid=${fid}, lastFid=${lastFid}`);
      }
      writer.appendUint8(delta << 4 | t & 15);
      writeElement(writer, t, v);
      lastFid = fid;
    }
    writer.appendUint8(CompactType.STOP);
  } else {
    throw new Error(`unhandled type in writeElement: ${type} for value ${value}`);
  }
}

// src/schema.js
function schemaFromColumnData({ columnData, schemaOverrides }) {
  const schema = [{
    name: "root",
    num_children: columnData.length
  }];
  let num_rows = 0;
  for (const { name, data, type, nullable } of columnData) {
    num_rows = num_rows || data.length;
    if (num_rows !== data.length) {
      throw new Error("columns must have the same length");
    }
    if (schemaOverrides?.[name]) {
      const override = schemaOverrides[name];
      if (override.name !== name) throw new Error("schema override name does not match column name");
      if (override.num_children) throw new Error("schema override cannot have children");
      if (override.repetition_type === "REPEATED") throw new Error("schema override cannot be repeated");
      schema.push(override);
    } else if (type) {
      schema.push(basicTypeToSchemaElement(name, type, nullable));
    } else {
      schema.push(autoSchemaElement(name, data));
    }
  }
  return schema;
}
function basicTypeToSchemaElement(name, type, nullable) {
  const repetition_type = nullable === false ? "REQUIRED" : "OPTIONAL";
  if (type === "STRING") {
    return { name, type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type };
  }
  if (type === "JSON") {
    return { name, type: "BYTE_ARRAY", converted_type: "JSON", repetition_type };
  }
  if (type === "TIMESTAMP") {
    return { name, type: "INT64", converted_type: "TIMESTAMP_MILLIS", repetition_type };
  }
  if (type === "UUID") {
    return { name, type: "FIXED_LEN_BYTE_ARRAY", type_length: 16, logical_type: { type: "UUID" }, repetition_type };
  }
  if (type === "FLOAT16") {
    return { name, type: "FIXED_LEN_BYTE_ARRAY", type_length: 2, logical_type: { type: "FLOAT16" }, repetition_type };
  }
  return { name, type, repetition_type };
}
function autoSchemaElement(name, values) {
  let type;
  let repetition_type = "REQUIRED";
  let converted_type = void 0;
  if (values instanceof Int32Array) return { name, type: "INT32", repetition_type };
  if (values instanceof BigInt64Array) return { name, type: "INT64", repetition_type };
  if (values instanceof Float32Array) return { name, type: "FLOAT", repetition_type };
  if (values instanceof Float64Array) return { name, type: "DOUBLE", repetition_type };
  for (const value of values) {
    if (value === null || value === void 0) {
      repetition_type = "OPTIONAL";
    } else {
      let valueType = void 0;
      if (value === true || value === false) valueType = "BOOLEAN";
      else if (typeof value === "bigint") valueType = "INT64";
      else if (Number.isInteger(value)) valueType = "INT32";
      else if (typeof value === "number") valueType = "DOUBLE";
      else if (value instanceof Uint8Array) valueType = "BYTE_ARRAY";
      else if (typeof value === "string") {
        valueType = "BYTE_ARRAY";
        if (type && !converted_type) throw new Error("mixed types not supported");
        converted_type = "UTF8";
      } else if (value instanceof Date) {
        valueType = "INT64";
        if (type && !converted_type) throw new Error("mixed types not supported");
        converted_type = "TIMESTAMP_MILLIS";
      } else if (typeof value === "object") {
        converted_type = "JSON";
        valueType = "BYTE_ARRAY";
      } else if (!valueType) throw new Error(`cannot determine parquet type for: ${value}`);
      if (type === void 0) {
        type = valueType;
      } else if (type === "INT32" && valueType === "DOUBLE") {
        type = "DOUBLE";
      } else if (type === "DOUBLE" && valueType === "INT32") {
        valueType = "DOUBLE";
      }
      if (type !== valueType) {
        throw new Error(`parquet cannot write mixed types: ${type} and ${valueType}`);
      }
    }
  }
  if (!type) {
    type = "BYTE_ARRAY";
    repetition_type = "OPTIONAL";
  }
  return { name, type, repetition_type, converted_type };
}
function getMaxRepetitionLevel(schemaPath) {
  let maxLevel = 0;
  for (const element of schemaPath) {
    if (element.repetition_type === "REPEATED") {
      maxLevel++;
    }
  }
  return maxLevel;
}
function getMaxDefinitionLevel(schemaPath) {
  let maxLevel = 0;
  for (const element of schemaPath.slice(1)) {
    if (element.repetition_type !== "REQUIRED") {
      maxLevel++;
    }
  }
  return maxLevel;
}

// src/datapage.js
function writeDataPageV2(writer, values, schemaPath, encoding, compressed) {
  const { name, type, type_length, repetition_type } = schemaPath[schemaPath.length - 1];
  if (!type) throw new Error(`column ${name} cannot determine type`);
  if (repetition_type === "REPEATED") throw new Error(`column ${name} repeated types not supported`);
  const levels = new ByteWriter();
  const { definition_levels_byte_length, repetition_levels_byte_length, num_nulls } = writeLevels(levels, schemaPath, values);
  const nonnull = values.filter((v) => v !== null && v !== void 0);
  const page = new ByteWriter();
  if (encoding === "RLE") {
    if (type !== "BOOLEAN") throw new Error("RLE encoding only supported for BOOLEAN type");
    page.appendUint32(nonnull.length);
    writeRleBitPackedHybrid(page, nonnull, 1);
  } else if (encoding === "PLAIN_DICTIONARY" || encoding === "RLE_DICTIONARY") {
    let maxValue = 0;
    for (const v of values) if (v > maxValue) maxValue = v;
    const bitWidth = Math.ceil(Math.log2(maxValue + 1));
    page.appendUint8(bitWidth);
    writeRleBitPackedHybrid(page, nonnull, bitWidth);
  } else {
    writePlain(page, nonnull, type, type_length);
  }
  let compressedPage = page;
  if (compressed) {
    compressedPage = new ByteWriter();
    snappyCompress(compressedPage, new Uint8Array(page.getBuffer()));
  }
  writePageHeader(writer, {
    type: "DATA_PAGE_V2",
    uncompressed_page_size: levels.offset + page.offset,
    compressed_page_size: levels.offset + compressedPage.offset,
    data_page_header_v2: {
      num_values: values.length,
      num_nulls,
      num_rows: values.length,
      encoding,
      definition_levels_byte_length,
      repetition_levels_byte_length,
      is_compressed: compressed
    }
  });
  writer.appendBuffer(levels.getBuffer());
  writer.appendBuffer(compressedPage.getBuffer());
}
function writePageHeader(writer, header) {
  const compact = {
    field_1: PageType.indexOf(header.type),
    field_2: header.uncompressed_page_size,
    field_3: header.compressed_page_size,
    field_4: header.crc,
    field_5: header.data_page_header && {
      field_1: header.data_page_header.num_values,
      field_2: Encoding.indexOf(header.data_page_header.encoding),
      field_3: Encoding.indexOf(header.data_page_header.definition_level_encoding),
      field_4: Encoding.indexOf(header.data_page_header.repetition_level_encoding)
      // field_5: header.data_page_header.statistics,
    },
    field_7: header.dictionary_page_header && {
      field_1: header.dictionary_page_header.num_values,
      field_2: Encoding.indexOf(header.dictionary_page_header.encoding)
    },
    field_8: header.data_page_header_v2 && {
      field_1: header.data_page_header_v2.num_values,
      field_2: header.data_page_header_v2.num_nulls,
      field_3: header.data_page_header_v2.num_rows,
      field_4: Encoding.indexOf(header.data_page_header_v2.encoding),
      field_5: header.data_page_header_v2.definition_levels_byte_length,
      field_6: header.data_page_header_v2.repetition_levels_byte_length,
      field_7: header.data_page_header_v2.is_compressed ? void 0 : false
      // default true
    }
  };
  serializeTCompactProtocol(writer, compact);
}
function writeLevels(writer, schemaPath, values) {
  let num_nulls = 0;
  const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath);
  let repetition_levels_byte_length = 0;
  if (maxRepetitionLevel) {
    repetition_levels_byte_length = writeRleBitPackedHybrid(writer, [], 0);
  }
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath);
  let definition_levels_byte_length = 0;
  if (maxDefinitionLevel) {
    const definitionLevels = [];
    for (const value of values) {
      if (value === null || value === void 0) {
        definitionLevels.push(maxDefinitionLevel - 1);
        num_nulls++;
      } else {
        definitionLevels.push(maxDefinitionLevel);
      }
    }
    const bitWidth = Math.ceil(Math.log2(maxDefinitionLevel + 1));
    definition_levels_byte_length = writeRleBitPackedHybrid(writer, definitionLevels, bitWidth);
  }
  return { definition_levels_byte_length, repetition_levels_byte_length, num_nulls };
}

// src/column.js
function writeColumn(writer, schemaPath, values, compressed, stats) {
  const element = schemaPath[schemaPath.length - 1];
  const { type, type_length } = element;
  if (!type) throw new Error(`column ${element.name} cannot determine type`);
  const offsetStart = writer.offset;
  const num_values = values.length;
  const encodings = [];
  const statistics = stats ? getStatistics(values) : void 0;
  let dictionary_page_offset;
  let data_page_offset = BigInt(writer.offset);
  const dictionary = useDictionary(values, type);
  if (dictionary) {
    dictionary_page_offset = BigInt(writer.offset);
    const indexes = new Array(values.length);
    for (let i = 0; i < values.length; i++) {
      if (values[i] !== null && values[i] !== void 0) {
        indexes[i] = dictionary.indexOf(values[i]);
      }
    }
    const unconverted = unconvert(element, dictionary);
    writeDictionaryPage(writer, unconverted, type, type_length, compressed);
    data_page_offset = BigInt(writer.offset);
    writeDataPageV2(writer, indexes, schemaPath, "RLE_DICTIONARY", compressed);
    encodings.push("RLE_DICTIONARY");
  } else {
    values = unconvert(element, values);
    const encoding = type === "BOOLEAN" && values.length > 16 ? "RLE" : "PLAIN";
    writeDataPageV2(writer, values, schemaPath, encoding, compressed);
    encodings.push(encoding);
  }
  return {
    type,
    encodings,
    path_in_schema: schemaPath.slice(1).map((s) => s.name),
    codec: compressed ? "SNAPPY" : "UNCOMPRESSED",
    num_values: BigInt(num_values),
    total_compressed_size: BigInt(writer.offset - offsetStart),
    total_uncompressed_size: BigInt(writer.offset - offsetStart),
    // TODO
    data_page_offset,
    dictionary_page_offset,
    statistics
  };
}
function useDictionary(values, type) {
  if (type === "BOOLEAN") return;
  const unique = new Set(values);
  unique.delete(void 0);
  unique.delete(null);
  if (values.length / unique.size > 2) {
    return Array.from(unique);
  }
}
function writeDictionaryPage(writer, dictionary, type, fixedLength, compressed) {
  const dictionaryPage = new ByteWriter();
  writePlain(dictionaryPage, dictionary, type, fixedLength);
  let compressedDictionaryPage = dictionaryPage;
  if (compressed) {
    compressedDictionaryPage = new ByteWriter();
    snappyCompress(compressedDictionaryPage, new Uint8Array(dictionaryPage.getBuffer()));
  }
  writePageHeader(writer, {
    type: "DICTIONARY_PAGE",
    uncompressed_page_size: dictionaryPage.offset,
    compressed_page_size: compressedDictionaryPage.offset,
    dictionary_page_header: {
      num_values: dictionary.length,
      encoding: "PLAIN"
    }
  });
  writer.appendBuffer(compressedDictionaryPage.getBuffer());
}
function getStatistics(values) {
  let min_value = void 0;
  let max_value = void 0;
  let null_count = 0n;
  for (const value of values) {
    if (value === null || value === void 0) {
      null_count++;
      continue;
    }
    if (min_value === void 0 || value < min_value) {
      min_value = value;
    }
    if (max_value === void 0 || value > max_value) {
      max_value = value;
    }
  }
  return { min_value, max_value, null_count };
}

// src/metadata.js
function writeMetadata(writer, metadata) {
  const compact = {
    field_1: metadata.version,
    field_2: metadata.schema && metadata.schema.map((element) => ({
      field_1: element.type && ParquetType.indexOf(element.type),
      field_2: element.type_length,
      field_3: element.repetition_type && FieldRepetitionType.indexOf(element.repetition_type),
      field_4: element.name,
      field_5: element.num_children,
      field_6: element.converted_type && ConvertedType.indexOf(element.converted_type),
      field_7: element.scale,
      field_8: element.precision,
      field_9: element.field_id,
      field_10: logicalType(element.logical_type)
    })),
    field_3: metadata.num_rows,
    field_4: metadata.row_groups.map((rg) => ({
      field_1: rg.columns.map((c, columnIndex) => ({
        field_1: c.file_path,
        field_2: c.file_offset,
        field_3: c.meta_data && {
          field_1: ParquetType.indexOf(c.meta_data.type),
          field_2: c.meta_data.encodings.map((e) => Encoding.indexOf(e)),
          field_3: c.meta_data.path_in_schema,
          field_4: CompressionCodec.indexOf(c.meta_data.codec),
          field_5: c.meta_data.num_values,
          field_6: c.meta_data.total_uncompressed_size,
          field_7: c.meta_data.total_compressed_size,
          field_8: c.meta_data.key_value_metadata && c.meta_data.key_value_metadata.map((kv) => ({
            field_1: kv.key,
            field_2: kv.value
          })),
          field_9: c.meta_data.data_page_offset,
          field_10: c.meta_data.index_page_offset,
          field_11: c.meta_data.dictionary_page_offset,
          field_12: c.meta_data.statistics && unconvertStatistics(c.meta_data.statistics, metadata.schema[columnIndex + 1]),
          field_13: c.meta_data.encoding_stats && c.meta_data.encoding_stats.map((es) => ({
            field_1: PageType.indexOf(es.page_type),
            field_2: Encoding.indexOf(es.encoding),
            field_3: es.count
          })),
          field_14: c.meta_data.bloom_filter_offset,
          field_15: c.meta_data.bloom_filter_length,
          field_16: c.meta_data.size_statistics && {
            field_1: c.meta_data.size_statistics.unencoded_byte_array_data_bytes,
            field_2: c.meta_data.size_statistics.repetition_level_histogram,
            field_3: c.meta_data.size_statistics.definition_level_histogram
          }
        },
        field_4: c.offset_index_offset,
        field_5: c.offset_index_length,
        field_6: c.column_index_offset,
        field_7: c.column_index_length,
        // field_8: c.crypto_metadata,
        field_9: c.encrypted_column_metadata
      })),
      field_2: rg.total_byte_size,
      field_3: rg.num_rows,
      field_4: rg.sorting_columns && rg.sorting_columns.map((sc) => ({
        field_1: sc.column_idx,
        field_2: sc.descending,
        field_3: sc.nulls_first
      })),
      field_5: rg.file_offset,
      field_6: rg.total_compressed_size
      // field_7: rg.ordinal, // should be int16
    })),
    field_5: metadata.key_value_metadata && metadata.key_value_metadata.map((kv) => ({
      field_1: kv.key,
      field_2: kv.value
    })),
    field_6: metadata.created_by
  };
  const metadataStart = writer.offset;
  serializeTCompactProtocol(writer, compact);
  const metadataLength = writer.offset - metadataStart;
  writer.appendUint32(metadataLength);
}
function logicalType(type) {
  if (!type) return;
  if (type.type === "STRING") return { field_1: {} };
  if (type.type === "MAP") return { field_2: {} };
  if (type.type === "LIST") return { field_3: {} };
  if (type.type === "ENUM") return { field_4: {} };
  if (type.type === "DECIMAL") return { field_5: {
    field_1: type.scale,
    field_2: type.precision
  } };
  if (type.type === "DATE") return { field_6: {} };
  if (type.type === "TIME") return { field_7: {
    field_1: type.isAdjustedToUTC,
    field_2: timeUnit(type.unit)
  } };
  if (type.type === "TIMESTAMP") return { field_8: {
    field_1: type.isAdjustedToUTC,
    field_2: timeUnit(type.unit)
  } };
  if (type.type === "INTEGER") return { field_10: {
    field_1: type.bitWidth,
    field_2: type.isSigned
  } };
  if (type.type === "NULL") return { field_11: {} };
  if (type.type === "JSON") return { field_12: {} };
  if (type.type === "BSON") return { field_13: {} };
  if (type.type === "UUID") return { field_14: {} };
  if (type.type === "FLOAT16") return { field_15: {} };
  if (type.type === "VARIANT") return { field_16: {} };
  if (type.type === "GEOMETRY") return { field_17: {} };
  if (type.type === "GEOGRAPHY") return { field_18: {} };
}
function timeUnit(unit) {
  if (unit === "NANOS") return { field_3: {} };
  if (unit === "MICROS") return { field_2: {} };
  return { field_1: {} };
}

// src/parquet-writer.js
function ParquetWriter({ writer, schema, compressed = true, statistics = true, kvMetadata }) {
  this.writer = writer;
  this.schema = schema;
  this.compressed = compressed;
  this.statistics = statistics;
  this.kvMetadata = kvMetadata;
  this.row_groups = [];
  this.num_rows = 0n;
  this.writer.appendUint32(827474256);
}
ParquetWriter.prototype.write = function({ columnData, rowGroupSize = 1e5 }) {
  const columnDataRows = columnData[0]?.data?.length || 0;
  for (let groupStartIndex = 0; groupStartIndex < columnDataRows; groupStartIndex += rowGroupSize) {
    const groupStartOffset = this.writer.offset;
    const groupSize = Math.min(rowGroupSize, columnDataRows - groupStartIndex);
    const columns = [];
    for (let j = 0; j < columnData.length; j++) {
      const { data } = columnData[j];
      const schemaPath = [this.schema[0], this.schema[j + 1]];
      const groupData = data.slice(groupStartIndex, groupStartIndex + groupSize);
      const file_offset = BigInt(this.writer.offset);
      const meta_data = writeColumn(this.writer, schemaPath, groupData, this.compressed, this.statistics);
      columns.push({
        file_offset,
        meta_data
      });
    }
    this.num_rows += BigInt(groupSize);
    this.row_groups.push({
      columns,
      total_byte_size: BigInt(this.writer.offset - groupStartOffset),
      num_rows: BigInt(groupSize)
    });
  }
};
ParquetWriter.prototype.finish = function() {
  const metadata = {
    version: 2,
    created_by: "hyparquet",
    schema: this.schema,
    num_rows: this.num_rows,
    row_groups: this.row_groups,
    metadata_length: 0,
    key_value_metadata: this.kvMetadata
  };
  delete metadata.metadata_length;
  writeMetadata(this.writer, metadata);
  this.writer.appendUint32(827474256);
  this.writer.finish();
};

// src/write.js
function parquetWrite({
  writer,
  columnData,
  schema,
  compressed = true,
  statistics = true,
  rowGroupSize = 1e5,
  kvMetadata
}) {
  if (!schema) {
    schema = schemaFromColumnData({ columnData });
  } else if (columnData.some(({ type }) => type)) {
    throw new Error("cannot provide both schema and columnData type");
  } else {
  }
  const pq = new ParquetWriter({
    writer,
    schema,
    compressed,
    statistics,
    kvMetadata
  });
  pq.write({
    columnData,
    rowGroupSize
  });
  pq.finish();
}
function parquetWriteBuffer(options) {
  const writer = new ByteWriter();
  parquetWrite({ ...options, writer });
  return writer.getBuffer();
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  ByteWriter,
  ParquetWriter,
  autoSchemaElement,
  parquetWrite,
  parquetWriteBuffer,
  schemaFromColumnData
});
