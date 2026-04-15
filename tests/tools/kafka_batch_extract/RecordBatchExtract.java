/*
 * librdkafka - Apache Kafka C library
 *
 * Extract one training sample per Kafka RecordBatch from log segment files.
 *
 * Output format:
 *   magic[8] = "RDKBAT01"
 *   u32 sample_count (little-endian)
 *   repeated sample_count times:
 *     u32 sample_len (little-endian)
 *     sample bytes
 *
 * Each sample is the reconstructed uncompressed records payload of a v2 batch,
 * matching the byte region librdkafka hands to outer batch compression.
 */

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

public final class RecordBatchExtract {
    private static final byte[] MAGIC =
            "RDKBAT01".getBytes(StandardCharsets.US_ASCII);

    private static final class Options {
        Path outputPath;
        final List<Path> inputPaths = new ArrayList<>();
        long maxSamples = Long.MAX_VALUE;
        boolean includeControlBatches = false;
    }

    private static final class Stats {
        long filesRead;
        long batchesSeen;
        long batchesWritten;
        long skippedControl;
        long skippedOldMagic;
        long skippedEmpty;
        long sampleBytes;
    }

    private RecordBatchExtract() {
    }

    public static void main(String[] args) throws Exception {
        Options opts = parseArgs(args);
        Stats stats = new Stats();

        try (RandomAccessFile out = new RandomAccessFile(opts.outputPath.toFile(), "rw")) {
            out.setLength(0);
            out.write(MAGIC);
            writeIntLE(out, 0);

            extractAll(opts, out, stats);

            if (stats.batchesWritten > Integer.MAX_VALUE) {
                throw new IllegalStateException(
                        "too many samples for u32 header: " + stats.batchesWritten);
            }

            out.seek(MAGIC.length);
            writeIntLE(out, (int) stats.batchesWritten);
        }

        System.out.printf(
                "wrote output=%s files=%d batches_seen=%d batches_written=%d "
                        + "sample_bytes=%d skipped_control=%d skipped_old_magic=%d "
                        + "skipped_empty=%d%n",
                opts.outputPath,
                stats.filesRead,
                stats.batchesSeen,
                stats.batchesWritten,
                stats.sampleBytes,
                stats.skippedControl,
                stats.skippedOldMagic,
                stats.skippedEmpty);
    }

    private static void extractAll(Options opts, RandomAccessFile out, Stats stats)
            throws IOException {
        outer:
        for (Path inputPath : opts.inputPaths) {
            stats.filesRead++;

            try (FileRecords records = FileRecords.open(inputPath.toFile(), false)) {
                for (FileChannelRecordBatch batch : records.batches()) {
                    byte[] sample;

                    stats.batchesSeen++;

                    batch.ensureValid();

                    if (!opts.includeControlBatches && batch.isControlBatch()) {
                        stats.skippedControl++;
                        continue;
                    }

                    if (batch.magic() != RecordBatch.MAGIC_VALUE_V2) {
                        stats.skippedOldMagic++;
                        continue;
                    }

                    sample = reconstructRecordsPayload(batch);
                    if (sample.length == 0) {
                        stats.skippedEmpty++;
                        continue;
                    }

                    writeIntLE(out, sample.length);
                    out.write(sample);
                    stats.batchesWritten++;
                    stats.sampleBytes += sample.length;

                    if (stats.batchesWritten >= opts.maxSamples)
                        break outer;
                }
            }
        }
    }

    private static byte[] reconstructRecordsPayload(RecordBatch batch)
            throws IOException {
        int estimatedSize = DefaultRecordBatch.RECORD_BATCH_OVERHEAD + 1024;

        try (CloseableIterator<Record> iter =
                     batch.streamingIterator(BufferSupplier.NO_CACHING)) {
            while (iter.hasNext())
                estimatedSize += iter.next().sizeInBytes();
        }

        if (estimatedSize <= DefaultRecordBatch.RECORD_BATCH_OVERHEAD)
            return new byte[0];

        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                buffer,
                batch.magic(),
                CompressionType.NONE,
                batch.timestampType(),
                batch.baseOffset());

        try (CloseableIterator<Record> iter =
                     batch.streamingIterator(BufferSupplier.NO_CACHING)) {
            while (iter.hasNext()) {
                Record record = iter.next();
                builder.appendWithOffset(record.offset(), record);
            }
        }

        MemoryRecords rebuilt = builder.build();
        MutableRecordBatch rebuiltBatch = rebuilt.batches().iterator().next();
        ByteBuffer rebuiltBuffer = rebuilt.buffer().duplicate();
        int recordsOffset = DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        int recordsLength = rebuiltBatch.sizeInBytes() - recordsOffset;
        byte[] sample = new byte[recordsLength];

        rebuiltBuffer.position(recordsOffset);
        rebuiltBuffer.get(sample);

        return sample;
    }

    private static Options parseArgs(String[] args) {
        Options opts = new Options();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            switch (arg) {
                case "--help":
                case "-h":
                    usage();
                    System.exit(0);
                    return null;
                case "--max-samples":
                    if (++i >= args.length)
                        throw new IllegalArgumentException("--max-samples needs a value");
                    opts.maxSamples = Long.parseLong(args[i]);
                    if (opts.maxSamples <= 0)
                        throw new IllegalArgumentException("--max-samples must be > 0");
                    break;
                case "--include-control-batches":
                    opts.includeControlBatches = true;
                    break;
                default:
                    if (arg.startsWith("-"))
                        throw new IllegalArgumentException("unknown option: " + arg);
                    if (opts.outputPath == null)
                        opts.outputPath = Path.of(arg);
                    else
                        opts.inputPaths.add(Path.of(arg));
                    break;
            }
        }

        if (opts.outputPath == null || opts.inputPaths.isEmpty()) {
            usage();
            throw new IllegalArgumentException(
                    "need OUTPUT followed by one or more SEGMENT.log files");
        }

        return opts;
    }

    private static void usage() {
        System.err.println(
                "Usage: RecordBatchExtract [options] OUTPUT INPUT.log [INPUT2.log ...]\n"
                        + "\n"
                        + "Options:\n"
                        + "  --max-samples N              Stop after writing N samples\n"
                        + "  --include-control-batches    Include control batches\n"
                        + "  --help, -h                   Show this help\n");
    }

    private static void writeIntLE(RandomAccessFile out, int value) throws IOException {
        out.write(value & 0xff);
        out.write((value >>> 8) & 0xff);
        out.write((value >>> 16) & 0xff);
        out.write((value >>> 24) & 0xff);
    }
}
