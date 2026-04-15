/*
 * librdkafka - Apache Kafka C library
 *
 * Extract individual Kafka records from log segment files into the existing
 * message corpus format used by 0208-payload_dict_mock.c.
 *
 * Output format:
 *   u32 record_count (little-endian)
 *   repeated record_count times:
 *     u32 key_len   (little-endian)
 *     u32 value_len (little-endian)
 *     key bytes
 *     value bytes
 */

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

public final class RecordExtract {
    private static final class Options {
        Path outputPath;
        final List<Path> inputPaths = new ArrayList<>();
        long maxRecords = Long.MAX_VALUE;
        boolean includeControlBatches = false;
    }

    private static final class Stats {
        long filesRead;
        long batchesSeen;
        long recordsSeen;
        long recordsWritten;
        long skippedControl;
        long skippedOldMagic;
        long keyBytes;
        long valueBytes;
    }

    private RecordExtract() {
    }

    public static void main(String[] args) throws Exception {
        Options opts = parseArgs(args);
        Stats stats = new Stats();

        try (RandomAccessFile out = new RandomAccessFile(opts.outputPath.toFile(), "rw")) {
            out.setLength(0);
            writeIntLE(out, 0);

            extractAll(opts, out, stats);

            if (stats.recordsWritten > Integer.MAX_VALUE) {
                throw new IllegalStateException(
                        "too many records for u32 header: " + stats.recordsWritten);
            }

            out.seek(0);
            writeIntLE(out, (int) stats.recordsWritten);
        }

        System.out.printf(
                "wrote output=%s files=%d batches_seen=%d records_seen=%d "
                        + "records_written=%d key_bytes=%d value_bytes=%d "
                        + "skipped_control=%d skipped_old_magic=%d%n",
                opts.outputPath,
                stats.filesRead,
                stats.batchesSeen,
                stats.recordsSeen,
                stats.recordsWritten,
                stats.keyBytes,
                stats.valueBytes,
                stats.skippedControl,
                stats.skippedOldMagic);
    }

    private static void extractAll(Options opts, RandomAccessFile out, Stats stats)
            throws IOException {
        outer:
        for (Path inputPath : opts.inputPaths) {
            stats.filesRead++;

            try (FileRecords records = FileRecords.open(inputPath.toFile(), false)) {
                for (FileChannelRecordBatch batch : records.batches()) {
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

                    try (CloseableIterator<Record> iter =
                                 batch.streamingIterator(BufferSupplier.NO_CACHING)) {
                        while (iter.hasNext()) {
                            Record record = iter.next();
                            byte[] key;
                            byte[] value;

                            stats.recordsSeen++;
                            record.ensureValid();

                            key = copyBytes(record.hasKey() ? record.key() : null);
                            value =
                                    copyBytes(record.hasValue() ? record.value() : null);

                            writeIntLE(out, key.length);
                            writeIntLE(out, value.length);
                            out.write(key);
                            out.write(value);

                            stats.recordsWritten++;
                            stats.keyBytes += key.length;
                            stats.valueBytes += value.length;

                            if (stats.recordsWritten >= opts.maxRecords)
                                break outer;
                        }
                    }
                }
            }
        }
    }

    private static byte[] copyBytes(ByteBuffer buffer) {
        ByteBuffer copy;
        byte[] data;

        if (buffer == null)
            return new byte[0];

        copy = buffer.duplicate();
        data = new byte[copy.remaining()];
        copy.get(data);
        return data;
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
                case "--max-records":
                    if (++i >= args.length)
                        throw new IllegalArgumentException("--max-records needs a value");
                    opts.maxRecords = Long.parseLong(args[i]);
                    if (opts.maxRecords <= 0)
                        throw new IllegalArgumentException("--max-records must be > 0");
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
                    "need OUTPUT followed by one or more segment files");
        }

        return opts;
    }

    private static void usage() {
        System.err.println(
                "Usage: RecordExtract [options] OUTPUT INPUT [INPUT2 ...]\n"
                        + "\n"
                        + "Options:\n"
                        + "  --max-records N              Stop after writing N records\n"
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
