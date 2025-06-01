/**
 * ParquetWriter class allows incremental writing of parquet files.
 *
 * @import {ColumnChunk, FileMetaData, KeyValue, RowGroup, SchemaElement} from 'hyparquet'
 * @import {ColumnSource, Writer} from '../src/types.js'
 * @param {object} options
 * @param {Writer} options.writer
 * @param {SchemaElement[]} options.schema
 * @param {boolean} [options.compressed]
 * @param {boolean} [options.statistics]
 * @param {KeyValue[]} [options.kvMetadata]
 */
export function ParquetWriter({ writer, schema, compressed, statistics, kvMetadata }: {
    writer: Writer;
    schema: SchemaElement[];
    compressed?: boolean | undefined;
    statistics?: boolean | undefined;
    kvMetadata?: import("hyparquet/src/types.js").KeyValue[] | undefined;
}): void;
export class ParquetWriter {
    /**
     * ParquetWriter class allows incremental writing of parquet files.
     *
     * @import {ColumnChunk, FileMetaData, KeyValue, RowGroup, SchemaElement} from 'hyparquet'
     * @import {ColumnSource, Writer} from '../src/types.js'
     * @param {object} options
     * @param {Writer} options.writer
     * @param {SchemaElement[]} options.schema
     * @param {boolean} [options.compressed]
     * @param {boolean} [options.statistics]
     * @param {KeyValue[]} [options.kvMetadata]
     */
    constructor({ writer, schema, compressed, statistics, kvMetadata }: {
        writer: Writer;
        schema: SchemaElement[];
        compressed?: boolean | undefined;
        statistics?: boolean | undefined;
        kvMetadata?: import("hyparquet/src/types.js").KeyValue[] | undefined;
    });
    writer: Writer;
    schema: import("hyparquet/src/types.js").SchemaElement[];
    compressed: boolean;
    statistics: boolean;
    kvMetadata: import("hyparquet/src/types.js").KeyValue[] | undefined;
    /** @type {RowGroup[]} */
    row_groups: RowGroup[];
    num_rows: bigint;
    /**
     * Write data to the file.
     * Will split data into row groups of the specified size.
     *
     * @param {object} options
     * @param {ColumnSource[]} options.columnData
     * @param {number} [options.rowGroupSize]
     */
    write({ columnData, rowGroupSize }: {
        columnData: ColumnSource[];
        rowGroupSize?: number | undefined;
    }): void;
    /**
     * Finish writing the file.
     */
    finish(): void;
}
import type { Writer } from '../src/types.js';
import type { SchemaElement } from 'hyparquet';
import type { RowGroup } from 'hyparquet';
import type { ColumnSource } from '../src/types.js';
//# sourceMappingURL=parquet-writer.d.ts.map