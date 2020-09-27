// DwarfIdea - offline network-based location format, tooling and libraries,
// see https://endl.ch/projects/dwarf-idea
//
// Copyright (C) 2019 - 2020 Alexander Tsvyashchenko <android@endl.ch>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package ch.endl.dwarf_idea.test;

import ch.endl.dwarf_idea.DwarfIdeaApi;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

public class DwarfIdeaApiTest {
    private static final int CacheSize = 1024;
    private static final int ShuffleSize = 100000;
    private static final int BlockCacheSize = 64;
    private static final int NumRandomLookups = 1000000;
    private static final int NumNegativeLookups = 10000000;
    private static final double EarthRadius = 6371000.0;

    private static class Entry {
        public Entry(byte[] key, float lat, float lon, byte[] extra_data) {
            this.key = key.clone();
            this.lat = lat;
            this.lon = lon;
            this.extra_data = extra_data != null? extra_data.clone() : null;
        }

        byte[] key;
        float lat, lon;
        byte[] extra_data;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    private static double max_dist_;
    private static double avg_dist_;
    private static int num_dist_;

    private static void testEntry(DwarfIdeaApi db, Entry entry) {
        DwarfIdeaApi.Result result = db.lookup(entry.key);
        if (result == null) {
            System.err.println("Key " + bytesToHex(entry.key) + ": not found, but should have been!");
            System.exit(1);
        } else {
            double dist = getDist(entry.lat, entry.lon, result.coords.lat, result.coords.lon);
            if (dist > db.getMaxDistError()) {
                System.err.println("Key " + bytesToHex(entry.key) + ": expected coords (" + entry.lat + ", " + entry.lon +
                        "), got (" + result.coords.lat + ", " + result.coords.lon + "), distance = " + dist);
                System.exit(1);
            } else {
                max_dist_ = Math.max(max_dist_, dist);
                avg_dist_ += dist;
                ++num_dist_;
            }
            boolean extra_data_matched = entry.extra_data == result.data ||
                    entry.extra_data.length == result.data.length;
            if (extra_data_matched && entry.extra_data != null) {
                for (int i = 0; i < result.data.length; ++i) {
                    if (entry.extra_data[i] != result.data[i]) {
                        extra_data_matched = false;
                        break;
                    }
                }
            }

            if (!extra_data_matched) {
                System.err.println("Key " + bytesToHex(entry.key) + ": " +
                        "expected extra data '" + bytesToHex(entry.extra_data) +
                        "', got " + bytesToHex(result.data));
                System.exit(1);
            }
        }
    }

    private static double getDist(double lat0, double lon0, double lat1, double lon1) {
        double sin_lat_2 = Math.sin((lat0 - lat1) * Math.PI / 180.0 / 2.0);
        double sin_lon_2 = Math.sin((lon0 - lon1) * Math.PI / 180.0 / 2.0);
        return EarthRadius * 2.0 * Math.asin(Math.sqrt(sin_lat_2 * sin_lat_2 +
                Math.cos(lat0 * Math.PI / 180.0) * Math.cos(lat1 * Math.PI / 180.0) *
                        sin_lon_2 * sin_lon_2));
    }

    private static class EntriesIterator implements Iterator<Entry> {
        public EntriesIterator(String filePath) throws FileNotFoundException {
            reader_ = new BufferedReader(new FileReader(filePath));
            entry_ = null;
            getNextEntry();
        }

        @Override
        public boolean hasNext() {
            return entry_ != null;
        }

        @Override
        public Entry next() {
            Entry current_entry = entry_;
            getNextEntry();
            return current_entry;
        }

        private void getNextEntry() {
            String line = null;
            try {
                line = reader_.readLine();
            } catch (IOException ex) {
                System.out.println(ex);
                System.exit(1);
            }

            if (line != null) {
                String[] rows = line.split("\\t");

                if (key_size_ == 0) {
                    key_size_ = rows[0].length() / 2;
                    key_ = new byte[key_size_];
                    if (rows.length > 3) {
                        extra_data_size_ = rows[3].length() / 2;
                        extra_data_ = new byte[extra_data_size_];
                    }
                }

                for (int i = 0; i < key_size_; ++i) {
                    key_[i] = (byte) Integer.parseInt(rows[0].substring(2 * i, 2 * i + 2), 16);
                }
                visited_.add(key_);

                float lat = (float) Double.parseDouble(rows[1]);
                float lon = (float) Double.parseDouble(rows[2]);

                for (int i = 0; i < extra_data_size_; ++i) {
                    extra_data_[i] = (byte) Integer.parseInt(rows[3].substring(2 * i, 2 * i + 2), 16);
                }

                entry_ = new Entry(key_, lat, lon, extra_data_);
            } else {
                entry_ = null;
            }
        }

        int key_size_ = 0, extra_data_size_ = 0;
        byte[] key_ = null;
        byte[] extra_data_ = null;
        HashSet<byte[]> visited_ = new HashSet<>();
        Entry entry_;
        BufferedReader reader_;
    }

    private static class EntriesRandomizedIterator implements Iterator<Entry> {
        public EntriesRandomizedIterator(EntriesIterator it, int buffer_size) throws FileNotFoundException {
            it_ = it;
            max_buffer_size_ = buffer_size;
            buffer_ = new ArrayList<>();
            fillBuffer();
        }

        @Override
        public boolean hasNext() {
            return cur_index_ < buffer_.size() || it_.hasNext();
        }

        @Override
        public Entry next() {
            Entry entry = buffer_.get(cur_index_++);
            if (cur_index_ == buffer_.size() - 1)
            {
                fillBuffer();
            }
            return entry;
        }

        private void fillBuffer()
        {
            buffer_.clear();
            while (it_.hasNext() && buffer_.size() < max_buffer_size_)
            {
                buffer_.add(it_.next());
            }
            Collections.shuffle(buffer_);
            cur_index_ = 0;
        }

        private EntriesIterator it_;
        private ArrayList<Entry> buffer_;
        private int cur_index_, max_buffer_size_;
    }

    public static void main(String[] args) throws IOException, DwarfIdeaApi.FileFormatException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: <db-file-path> <sample-csv-file-path>");
        }
        DwarfIdeaApi db = new DwarfIdeaApi(args[0], CacheSize, BlockCacheSize);
        final String csv_path = args[1];
        int num_entries = 0;

        long start_time = System.currentTimeMillis();
        EntriesIterator it = new EntriesIterator(csv_path);
        max_dist_ = 0.0;
        avg_dist_ = 0.0;
        num_dist_ = 0;
        while (it.hasNext()) {
            testEntry(db, it.next());
            ++num_entries;
        }
        System.out.println(
                "Sequential scan for " + num_entries + " entries took " +
                        (System.currentTimeMillis() - start_time) + "ms" +
                        ", max dist = " + max_dist_ + ", avg dist = " +
                        avg_dist_ / num_dist_);

        HashSet<byte[]> visited = it.visited_;
        byte[] key = it.key_, part_key = it.key_.clone();
        int key_size = it.key_size_;
        // For cells, skip randomization of MCC / MNC part as otherwise
        // most of the queries will be rejected @ key mapping stage.
        int rand_offset = key_size == 10 ? 4 : 0;
        Random rnd = new Random();
        rnd.setSeed(0);

        start_time = System.currentTimeMillis();
        it = new EntriesIterator(csv_path);
        EntriesRandomizedIterator itRand = new EntriesRandomizedIterator(it, ShuffleSize);
        for (int i = 0; i < NumRandomLookups; ++i) {
            if (!itRand.hasNext()) {
                System.err.println("Unexpected end of random entries stream!");
                System.exit(1);
            }
            testEntry(db, itRand.next());
        }
        System.out.println("Random scan for " + NumRandomLookups + " entities took " + (System.currentTimeMillis() - start_time) + "ms");

        start_time = System.currentTimeMillis();
        it = new EntriesIterator(csv_path);
        itRand = new EntriesRandomizedIterator(it, ShuffleSize);
        for (int i = 0; i < NumNegativeLookups; ++i) {
            // Choose with equal probability either to use completely
            // random key or mutation of already existing one.
            if (rnd.nextBoolean()) {
                rnd.nextBytes(key);
            } else {
                // Take existing key and replace its last octets, so that
                // MCCs / MNCs are valid and key mapping succeeds.
                if (!itRand.hasNext()) {
                    System.err.println("Unexpected end of random entries stream!");
                    System.exit(1);
                }
                System.arraycopy(itRand.next().key, 0, key, 0, key.length);
                rnd.nextBytes(part_key);
                int repl_start_pos = rand_offset + rnd.nextInt(key_size - rand_offset - 1);
                System.arraycopy(part_key, 0, key, repl_start_pos, key_size - repl_start_pos);
            }
            if (!visited.contains(key)) {
                DwarfIdeaApi.Result result = db.lookup(key);
                if (result != null) {
                    System.err.println("Key " + key + ": found, but should NOT have been!");
                    System.exit(1);
                }
            }
        }
        System.out.println(
                "Non-existing items scan for " + NumNegativeLookups +
                " entries took " + (System.currentTimeMillis() - start_time) + "ms");
    }
}
