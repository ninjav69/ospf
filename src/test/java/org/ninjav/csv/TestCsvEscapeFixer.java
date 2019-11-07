package org.ninjav.csv;

import com.opencsv.*;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvMalformedLineException;
import org.junit.Test;

import java.io.*;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.List;

public class TestCsvEscapeFixer {

    @Test
    public void testBufferedCsvEscapeFixer() throws IOException {
        Reader r = new CsvEscapeFixer(new FileReader("/home/ninjav/test.csv"));
        int ch = r.read();
        while (ch != -1) {
            System.out.print((char) ch);
            ch = r.read();
        }
    }

    @Test(expected = CsvMalformedLineException.class)
    public void testOpenCsv() throws IOException, CsvException {
        final CSVReader csv = new CSVReaderBuilder(
                new FileReader("/home/ninjav/test.csv"))
                .withCSVParser(new RFC4180Parser())
                .withSkipLines(1)
                .build();
        List<String[]> result = csv.readAll();
        result.forEach(l -> {
            Arrays.stream(l).forEach(s -> System.out.print(s + ","));
            System.out.println();
        });
    }

    @Test
    public void testOpenCsv_withBufferedCsvEscapeFixer() throws IOException, CsvException {
        final CSVReader csv = new CSVReaderBuilder(
                new CsvEscapeFixer(
                        new FileReader("/home/ninjav/test.csv")))
                .withCSVParser(new RFC4180Parser())
                .withSkipLines(1)
                .build();
        List<String[]> result = csv.readAll();
        result.forEach(l -> {
            Arrays.stream(l).forEach(s -> System.out.print(s + ","));
            System.out.println();
        });

    }

    @Test
    public void withTabDelimitedCsv_testOpenCsv() throws IOException, CsvException {
        final CSVReader csv = new CSVReaderBuilder(
                new FileReader("/home/ninjav/test.csv"))
                .withCSVParser(new CSVParserBuilder()
                        .withStrictQuotes(false)
                        .withIgnoreQuotations(true)
                        .build())
                .withSkipLines(1)
                .build();
        List<String[]> result = csv.readAll();
        result.forEach(l -> {
            Arrays.stream(l).forEach(s -> System.out.print(s + "|"));
            System.out.println();
        });
    }


    public static class CsvEscapeFixer extends Reader {

        final Reader reader;
        final CharBuffer buf = CharBuffer.allocate(8192 * 2);

        static final int OUT_CELL = 1;
        static final int IN_CELL = 2;
        int state = OUT_CELL;

        boolean done = false;

        public CsvEscapeFixer(Reader reader) {
            this.reader = new BufferedReader(reader);
        }

        // Read some data from the reader, fixing quotes as we go. Store output in buf.
        private int readChunk(int len) throws IOException {
            int in;
            int rchars = 0;
            while ((in = reader.read()) != -1) {
                char ch = (char) in;
                reader.mark(2);
                char next = (char) reader.read();
                reader.reset();

                if (ch == '"' && state == OUT_CELL) {
                    buf.put(ch);
                    state = IN_CELL;
                } else if (ch == '"' && state == IN_CELL) {
                    buf.put(ch);
                    if (next == ',' || next == '\r' || next == '\n') {
                        state = OUT_CELL;
                    } else {
                        buf.put('"');
                    }
                } else {
                    buf.put(ch);
                }
                if (++rchars >= len) {
                    break;
                }
            }
            return rchars;
        }

        @Override
        public int read(char[] chars, int offset, int len) throws IOException {
            if (!done) {
                int nchars = readChunk(len);
                if (nchars == 0) {
                    done = true;
                }
                return handBack(chars, len);
            } else {
                if (buf.hasRemaining()) {
                    return handBack(chars, len);
                } else {
                    return -1;
                }
            }
        }

        int handBack(char[] chars, int len) {
            int written = 0;
            buf.flip();
            while (buf.hasRemaining() && (written < len)) {
                chars[written++] = buf.get();
            }
            buf.compact();
            return written - 1;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }


    @Test
    public void testFileReader() throws IOException {
        BufferedReader r = new BufferedReader(new CsvEscapeFixer(new FileReader("/home/ninjav/test2.csv")));
        String line;
        while ((line = r.readLine()) != null) {
            System.out.println(line);
        }
    }


}
