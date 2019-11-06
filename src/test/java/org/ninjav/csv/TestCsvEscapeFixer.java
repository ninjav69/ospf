package org.ninjav.csv;

import org.junit.Test;

import java.io.*;
import java.nio.CharBuffer;

public class TestCsvEscapeFixer {

    final CharBuffer cout = CharBuffer.allocate(8192 * 2);
    static final int OUT_CELL = 1;
    static final int IN_CELL = 2;
    int state = OUT_CELL;

    @Test
    public void reacChars() throws IOException {
        Reader r = new BufferedReader(new FileReader("/home/ninjav/test.csv"));

        int read;
        while ((read = r.read()) != -1) {
            char ch = (char)read;
            r.mark(2);
            char next = (char)r.read();
            r.reset();

            if (ch == '"' && state == OUT_CELL) {
                cout.append(ch);
                state = IN_CELL;
            } else if (ch == '"' && state == IN_CELL) {
                if (next == ',' || next == '\r' || next == '\n') {
                    state = OUT_CELL;
                    cout.append(ch);
                } else {
                    cout.append('"');
                    cout.append('"');
                }
            } else {
                cout.append(ch);
            }
        }
        r.close();

        System.out.println("CSV:");
        System.out.println(new String(cout.array()));
    }

    @Test
    public void testGreedyCsvEscapeFixer() throws IOException {
        Reader r = new GreedyCsvEscapeFixer(new FileReader("/home/ninjav/test.csv"));
        int read = r.read();
        while (read != -1) {
            System.out.print((char) read);
            read = r.read();
        }
        r.close();
    }

    public static class GreedyCsvEscapeFixer extends Reader {

        final Reader reader;
        final CharBuffer cout = CharBuffer.allocate(8192 * 2);

        static final int OUT_CELL = 1;
        static final int IN_CELL = 2;
        int state = OUT_CELL;
        int charcount = 0;

        boolean initialized = false;
        boolean done = false;

        public GreedyCsvEscapeFixer(Reader reader) {
            this.reader = new BufferedReader(reader);
        }

        // Read all data from the reader, fixing quotes as we go. Store output in cout.
        private void initialize() throws IOException {
            int read;
            while ((read = reader.read()) != -1) {
                char ch = (char)read;
                reader.mark(2);
                char next = (char)reader.read();
                reader.reset();

                if (ch == '"' && state == OUT_CELL) {
                    cout.put(ch);
                    state = IN_CELL;
                } else if (ch == '"' && state == IN_CELL) {
                    if (next == ',' || next == '\r' || next == '\n') {
                        state = OUT_CELL;
                        cout.put(ch);
                    } else {
                        cout.put('"');
                        cout.put('"');
                    }
                } else {
                    cout.put(ch);
                }
            }
            initialized = true;
        }

        @Override
        public int read(char[] chars, int offset, int len) throws IOException {
            if (done) {
                return -1;
            }
            if (!initialized) {
                initialize();
                charcount = cout.position() - 1;
                cout.position(0);
            }
            // Systematically hand back chunks of cout until empty.
            int count = len;
            if (charcount - cout.position() < len) {
                count = charcount - cout.position() - 1;
            }

            for (int i = 0; i < count; i++) {
                chars[i] = cout.get();
            }
            if (count < len) {
                done = true;
            }
            return count;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    @Test
    public void testBufferedCsvExcapeFixer() throws IOException {
        Reader r = new BufferedCsvEscapeFixer(new FileReader("/home/ninjav/test.csv"));
        int ch = r.read();
        while (ch != -1) {
            System.out.print((char) ch);
            ch = r.read();
        }
    }


    public static class BufferedCsvEscapeFixer extends Reader {

        final Reader reader;
        final CharBuffer cout = CharBuffer.allocate(8192 * 2);

        static final int OUT_CELL = 1;
        static final int IN_CELL = 2;
        int state = OUT_CELL;

        boolean done = false;

        public BufferedCsvEscapeFixer(Reader reader) {
            this.reader = new BufferedReader(reader);
        }

        // Keep track of chars read/written
        private static class Stats {
            public int rchars = 0;
            public int wchars = 0;
        }

        // Read some data from the reader, fixing quotes as we go. Store output in cout.
        private Stats readChunk(int len) throws IOException {
            int in;
            Stats s = new Stats();
            while ((in = reader.read()) != -1) {
                char ch = (char)in;
                reader.mark(2);
                char next = (char)reader.read();
                reader.reset();

                if (ch == '"' && state == OUT_CELL) {
                    cout.put(ch);
                    s.wchars++;
                    state = IN_CELL;
                } else if (ch == '"' && state == IN_CELL) {
                    cout.put(ch);
                    s.wchars++;
                    if (next == ',' || next == '\r' || next == '\n') {
                        state = OUT_CELL;
                    } else {
                        cout.put('"');
                        s.wchars++;
                    }
                } else {
                    cout.put(ch);
                    s.wchars++;
                }
                if (++s.rchars >= len) {
                    break;
                }
            }
            return s;
        }

        @Override
        public int read(char[] chars, int offset, int len) throws IOException {
            if (done) {
                return -1;
            }

            Stats s = readChunk(len);
            cout.position(0);
            for (int i = 0; i < s.rchars; i++) {
                chars[i] = cout.get();
            }
            cout.position(s.wchars - s.rchars);
            if (s.rchars < len && s.wchars < len) {
                done = true;
            }
            if (s.rchars == 0 && s.wchars > 0) {
                return s.rchars;
            }
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

}
