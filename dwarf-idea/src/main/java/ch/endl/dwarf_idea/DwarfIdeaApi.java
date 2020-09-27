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

package ch.endl.dwarf_idea;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import kanzi.SliceByteArray;
import kanzi.bitstream.DefaultInputBitStream;
import kanzi.function.ZRLT;
import kanzi.transform.BWTS;
import kanzi.transform.SBRT;

import static kanzi.transform.SBRT.MODE_RANK;

public class DwarfIdeaApi {
  public static class FileFormatException extends Exception {
    public FileFormatException(String message) {
      super(message);
    }
  }

  public static class Coords {
    public float lat, lon;

    public Coords() {
      this.lat = 0.0f;
      this.lon = 0.0f;
    }

    public Coords(float lat, float lon) {
      this.lat = lat;
      this.lon = lon;
    }
  }

  public static class Result {
    public Coords coords;
    public byte[] data;
  }

  public DwarfIdeaApi(String file_name, int cached_entries, int cached_blocks) throws IOException, FileFormatException {
    results_cache_ = new LRUCache<>(cached_entries);
    keys_cache_ = new LRUCache<>(cached_blocks);
    coords_cache_ = new LRUCache<>(cached_blocks);
    extra_data_cache_ = new LRUCache<>(cached_blocks);
    pos_stack_ = new Stack<>();
    bwts_ = new BWTS();
    sbrt_ = new SBRT(MODE_RANK);
    zrlt_ = new ZRLT();
    key_map_ = null;
    keys_fse_ = null;
    coords_fse_ = null;
    extra_data_fse_ = null;

    File file = new File(file_name);
    int db_length = (int)file.length();

    db_ = new FileInputStream(file);
    buffer_ = db_.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, db_length);
    buffer_.order(ByteOrder.LITTLE_ENDIAN);

    byte[] sig_data = new byte[FileSignature.length()];
    buffer_.get(sig_data, 0, FileSignature.length());
    String signature = new String(sig_data, StandardCharsets.US_ASCII);
    if (!signature.equals(FileSignature)) {
      throw new FileFormatException("Unexpected signature: " + signature);
    }

    short version = buffer_.getShort();
    if (version != SupportedVersion) {
      throw new FileFormatException("Unexpected DwarfIdea version: " + version);
    }

    key_size_ = buffer_.getShort();
    extra_data_size_ = buffer_.getShort();
    num_entries_ = buffer_.getInt();
    index_size_ = buffer_.getInt();
    min_entries_per_block_ = buffer_.getShort();
    max_entries_per_block_ = buffer_.getShort();
    bounding_box_bits_ = buffer_.getShort();
    max_dist_error_ = buffer_.getFloat();
    short key_map_size = buffer_.getShort();

    // TODO: calculate this number more intelligently instead of using
    // guesstimated upper bound or dynamically reallocate as needed.
    int max_len = 32 * max_entries_per_block_;
    tmp_buffer_ = ByteBuffer.allocate(max_len);
    tmp_buffer_as_slice_array_ = new SliceByteArray(tmp_buffer_.array(), max_len, 0);
    slice_array_ = new SliceByteArray(new byte[max_len], max_len, 0);
    slice_array2_ = new SliceByteArray(new byte[max_len], max_len, 0);

    if (key_map_size > 0) {
      key_map_ = new HashMap<>();
      for (short i = 0; i < key_map_size; ++i) {
        key_map_.put(buffer_.getInt(), i);
      }
      // TODO: hardcoded case for cells mapping, generalize?
      key_size_ -= 2;
    }

    byte[] last_key_data = new byte[key_size_];
    buffer_.get(last_key_data, 0, key_size_);
    last_key_ = new Key(last_key_data);

    long bounding_box_max_index = (1 << bounding_box_bits_) - 1;
    bounding_box_lat_step_ = (kMaxLat - kMinLat) / bounding_box_max_index;
    bounding_box_lon_step_ = (kMaxLon - kMinLon) / bounding_box_max_index;

    int keys_freq_size = buffer_.getInt();
    pushPosition();
    keys_fse_ = new FiniteStateEntropy(buffer_, buffer_.position(), keys_freq_size);
    popPosition(keys_freq_size);

    int coords_freq_size = buffer_.getInt();
    pushPosition();
    coords_fse_ = new FiniteStateEntropy(buffer_, buffer_.position(), coords_freq_size);
    popPosition(coords_freq_size);

    if (extra_data_size_ > 0) {
      int extra_data_freq_size = buffer_.getInt();
      pushPosition();
      extra_data_fse_ = new FiniteStateEntropy(buffer_, buffer_.position(), extra_data_freq_size);
      popPosition(extra_data_freq_size);
    }

    index_offset_ = buffer_.position();
  }

  public float getMaxDistError() {
    return max_dist_error_;
  }

  public Result lookup(byte[] key) {
    Key orig_key = new Key(key);
    if (results_cache_.containsKey(orig_key)) {
      return results_cache_.get(orig_key);
    }

    Key mapped_key = mapKey(orig_key);
    if (mapped_key == null) {
      results_cache_.put(orig_key, null);
      return null;
    }

    if (compareULong(last_key_.asInt(), mapped_key.asInt()) < 0) {
      results_cache_.put(orig_key, null);
      return null;
    }

    IndexSearchResult isr = findMatchingBlock(mapped_key);

    if (isr.block_index == -1) {
      results_cache_.put(orig_key, null);
      return null;
    }

    buffer_.position(isr.block_offset_pos);
    int block_pos = buffer_.getInt();
    buffer_.position(block_pos);

    try {
      int block_key_index = 0;
      int keys_data_size = decodeVarInt();
      int keys_data_pos = buffer_.position();
      if (!isr.exact_match) {
        byte[] encoded_keys = keys_cache_.getOrDefault(isr.block_index, null);
        if (encoded_keys == null) {
          encoded_keys = decompressBytes(keys_fse_, keys_data_size);
          keys_cache_.put(isr.block_index, encoded_keys);
        }
        block_key_index = findBlockKeyIndex(encoded_keys, isr.index_key, mapped_key);
      }

      buffer_.position(keys_data_pos + (keys_data_size >> 2));

      Result result = null;
      if (block_key_index != -1) {
        result = new Result();
        int coords_data_size = decodeVarInt();
        int coords_data_pos = buffer_.position();
        byte[] encoded_coords = coords_cache_.getOrDefault(isr.block_index, null);
        if (encoded_coords == null) {
          encoded_coords = decompressBytes(coords_fse_, coords_data_size);
          coords_cache_.put(isr.block_index, encoded_coords);
        }
        result.coords = decodeCoords(encoded_coords, block_key_index);
        buffer_.position(coords_data_pos + (coords_data_size >> 2));

        if (extra_data_size_ > 0) {
          int extra_data_data_size = decodeVarInt();
          int extra_data_data_pos = buffer_.position();
          byte[] encoded_extra_data = extra_data_cache_.getOrDefault(isr.block_index, null);
          if (encoded_extra_data == null) {
            encoded_extra_data = decompressBytes(extra_data_fse_, extra_data_data_size);
            extra_data_cache_.put(isr.block_index, encoded_extra_data);
          }
          result.data = decodeExtraData(encoded_extra_data, block_key_index);
          buffer_.position(extra_data_data_pos + (extra_data_data_size >> 2));
        } else {
          result.data = null;
        }
      }

      results_cache_.put(orig_key, result);
      return result;
    } catch (FileFormatException ex) {
    }
    return null;
  }

  public void close() {
    buffer_ = null;
    try {
      db_.close();
    } catch(Exception e) {
      // Not that much we can do anyway.
    }
    db_ = null;
  }

  private void pushPosition() {
    pos_stack_.push(buffer_.position());
  }

  private void popPosition(int offset) {
    buffer_.position(pos_stack_.pop() + offset);
  }

  private int decodeVarInt() {
    int result = 0, shift = 0;
    while (true) {
      int value = buffer_.get();
      if (value < 0) {
        result |= (value & 0x7F) << shift;
        shift += 7;
      } else {
        result |= value << shift;
        break;
      }
    }
    return result;
  }

  long compareULong(long value0, long value1) {
    return Long.compare(value0 + Long.MIN_VALUE, value1 + Long.MIN_VALUE);
  }

  private byte[] asBytesBigEndian(short value) {
    return new byte[] { (byte)((value >> 8) & 0xFF), (byte)(value & 0xFF) };
  }

  private Key mapKey(Key orig_key) {
    if (key_map_ != null) {
        // TODO: hardcoded case for cells mapping, generalize?
        int mcc = ((int)orig_key.data[1] & 0xFF) | (((int)orig_key.data[0] & 0xFF) << 8);
        int mnc = ((int)orig_key.data[3] & 0xFF) | (((int)orig_key.data[2] & 0xFF) << 8);
        int key_value = (mcc << 16) | mnc;
        if (!key_map_.containsKey(key_value)) {
          return null;
        }
        Key result = new Key(orig_key.data.length - 2);
        System.arraycopy(asBytesBigEndian(key_map_.get(key_value)),
                0, result.data, 0, 2);
        System.arraycopy(orig_key.data, 4, result.data,
                2, orig_key.data.length - 4);
        return result;
    } else {
      return orig_key;
    }
  }

  private IndexSearchResult findMatchingBlock(Key mapped_key) {
    // Find the matching block:
    int max_index = index_size_ - 1;
    int low = 0, high = max_index;
    long mapped_key_int = mapped_key.asInt();
    Key cur_key = new Key(mapped_key.data.length);
    int block_offset_pos = -1;
    int carry = 0;
    while (low < high) {
      int mid = (low + high + carry) / 2;
      buffer_.position(index_offset_ + mid * (key_size_ + 4));
      buffer_.get(cur_key.data, 0, key_size_);
      long cmp = compareULong(cur_key.asInt(), mapped_key_int);
      if (cmp > 0) {
        high = mid - 1;
      } else if (cmp < 0) {
        low = mid;
        if (low + 1 == high) {
          carry = 1;
        }
      } else {
        low = mid;
        high = mid;
      }
    }
    if (high >= 0 && low <= max_index) {
      buffer_.position(index_offset_ + low * (key_size_ + 4));
      buffer_.get(cur_key.data, 0, key_size_);
      block_offset_pos = buffer_.position();
      return new IndexSearchResult(
              low, block_offset_pos, cur_key,
              cur_key.asInt() == mapped_key_int);
    } else {
      return new IndexSearchResult(-1, -1, null, false);
    }
  }

  private int findBlockKeyIndex(byte[] encoded_keys, Key index_key, Key mapped_key) {
    // Decode the keys until we either find the one matching 'mapped_key',
    // find the one bigger than 'mapped_key' or run out of data.
    long mapped_key_int = mapped_key.asInt();
    long prev_key_int = index_key.asInt();
    MutableInt offset = new MutableInt();
    offset.value = 0;
    for (int cur_key_index = 1; offset.value < encoded_keys.length; ++cur_key_index) {
      long decoded_key_int = decodeKey(encoded_keys, offset);
      decoded_key_int += prev_key_int;
      long cmp = compareULong(decoded_key_int, mapped_key_int);
      if (cmp == 0) {
        return cur_key_index;
      } else if (cmp > 0) {
        // Current decoded key is already bigger than the key we search for, cancel the search.
        return -1;
      }
      prev_key_int = decoded_key_int;
    }
    // The key we search for is bigger than the last key we have in the block.
    return -1;
  }

  byte[] decompressBytes(FiniteStateEntropy fse, int size) throws FileFormatException {
    tmp_buffer_.position(0);
    int cur_pos = buffer_.position();
    boolean ignore_zrlt = (size & 0x01) != 0;
    boolean ignore_fse = (size & 0x02) != 0;
    size >>= 2;
    if (ignore_fse) {
      buffer_.get(tmp_buffer_.array(), 0, size);
    } else {
      size = fse.decompress(buffer_, cur_pos, size, tmp_buffer_);
      if (size <= 0) {
        throw new FileFormatException("Couldn't FSE-decode block @ " + cur_pos);
      }
    }

    if (ignore_zrlt) {
      System.arraycopy(tmp_buffer_.array(), 0, slice_array_.array, 0, size);
      slice_array_.index = size;
    } else {
      tmp_buffer_as_slice_array_.length = size;
      tmp_buffer_as_slice_array_.index = 0;
      slice_array_.length = slice_array_.array.length;
      slice_array_.index = 0;
      if (!zrlt_.inverse(tmp_buffer_as_slice_array_, slice_array_)) {
        throw new FileFormatException("Couldn't ZRLT-decode block @ " + cur_pos);
      }
    }

    slice_array_.length = slice_array_.index;
    slice_array_.index = 0;
    slice_array2_.length = slice_array2_.array.length;
    slice_array2_.index = 0;
    if (!sbrt_.inverse(slice_array_, slice_array2_)) {
      throw new FileFormatException("Couldn't SBRT-decode block @ " + cur_pos);
    }

    slice_array2_.length = slice_array2_.index;
    slice_array2_.index = 0;
    slice_array_.length = slice_array_.array.length;
    slice_array_.index = 0;
    if (!bwts_.inverse(slice_array2_, slice_array_)) {
      throw new FileFormatException("Couldn't BWTS-decode block @ " + cur_pos);
    }

    return Arrays.copyOfRange(slice_array_.array, 0, slice_array_.index);
  }

  // TODO: this is now almost exactly the same as decodeVarInt,
  // unify these (e.g. wrap encoded_keys into ByteBuffer).
  private long decodeKey(byte[] encoded_keys, MutableInt offset) {
    long acc = 0;
    int shift = 0;
    for (; offset.value < encoded_keys.length; ++offset.value) {
      long value = encoded_keys[offset.value];
      if (value < 0) {
        acc |= (value & 0x7F) << shift;
        shift += 7;
      } else {
        acc |= value << shift;
        ++offset.value;
        break;
      }
    }
    return acc;
  }

  private Coords decodeCoords(byte[] encoded_coords, int block_key_index) {
    Coords coords = new Coords();

    DefaultInputBitStream bs = new DefaultInputBitStream(
            new ByteArrayInputStream(encoded_coords), 1024);

    BlockInfo block_info = new BlockInfo();
    block_info.lat_min_index = bs.readBits(bounding_box_bits_);
    block_info.lon_min_index = bs.readBits(bounding_box_bits_);
    block_info.lat_max_index = bs.readBits(bounding_box_bits_);
    block_info.lon_max_index = bs.readBits(bounding_box_bits_);
    block_info.lat_bits = (byte)bs.readBits(kCoordSpecBits);
    block_info.lon_bits = (byte)bs.readBits(kCoordSpecBits);
    block_info.coords_bits = (byte)(block_info.lat_bits + block_info.lon_bits);
    block_info.min_corner = new Coords(
            block_info.lat_min_index * bounding_box_lat_step_ + kMinLat,
            block_info.lon_min_index * bounding_box_lon_step_ + kMinLon);
    block_info.max_corner = new Coords(
            block_info.lat_max_index * bounding_box_lat_step_ + kMinLat,
            block_info.lon_max_index * bounding_box_lon_step_ + kMinLon);
    block_info.max_lat_diff = block_info.max_corner.lat - block_info.min_corner.lat;
    block_info.max_lon_diff = block_info.max_corner.lon - block_info.min_corner.lon;

    bs.skip(block_key_index * block_info.coords_bits);
    long combined = bs.readBits(block_info.coords_bits);

    int lat_idx = (int)(combined & ((1 << block_info.lat_bits) - 1));
    int lon_idx = (int)(combined >> block_info.lat_bits) & ((1 << block_info.lon_bits) - 1);

    float min_corner_lat = bounding_box_lat_step_ * block_info.lat_min_index + kMinLat;
    float min_corner_lon = bounding_box_lon_step_ * block_info.lon_min_index + kMinLon;

    coords.lat = (float)(min_corner_lat + block_info.max_lat_diff * lat_idx / ((1 << block_info.lat_bits) - 1));
    coords.lon = (float)(min_corner_lon + block_info.max_lon_diff * lon_idx / ((1 << block_info.lon_bits) - 1));

    return coords;
  }

  byte[] decodeExtraData(byte[] encoded_data, int block_key_index) {
    int ind = block_key_index * extra_data_size_;
    return Arrays.copyOfRange(encoded_data, ind, ind + extra_data_size_);
  }

  private static class MutableInt {
    int value;
  }

  private static class Key {
    private byte[] data;

    public Key(int len) { this.data = new byte[len]; }
    public Key(byte[] data) {
      this.data = data.clone();
    }

    @Override
    public boolean equals(Object object) {
      if (!(object instanceof Key)) {
        return false;
      }

      Key other_key = (Key)object;
      return Arrays.equals(data, other_key.data);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }

    long asInt() {
      if (data.length > 8) {
        throw new IllegalArgumentException("Key size is too large");
      }
      long int_val = 0;
      for (int i = 0; i < data.length; ++i)
      {
        int_val |= (long)(data[data.length - i - 1] & 0xFF) << (8 * i);
      }
      return int_val;
    }
  }

  private static class BlockInfo {
    long lat_min_index, lon_min_index, lat_max_index, lon_max_index;
    Coords min_corner, max_corner;
    double max_lat_diff, max_lon_diff;
    byte lat_bits, lon_bits, coords_bits;
  }

  private static class IndexSearchResult {
    public IndexSearchResult(int block_index, int block_offset_pos, Key index_key, boolean exact_match) {
      this.block_index = block_index;
      this.block_offset_pos = block_offset_pos;
      this.index_key = index_key;
      this.exact_match = exact_match;
    }

    public int block_index, block_offset_pos;
    public Key index_key;
    boolean exact_match;
  }

  private static class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private int cached_entries;
  
    public LRUCache(int cached_entries) {
      // Default arguments except of the last one, which
      // selects 'access-order' for removal.
      super(16, 0.75f, true);
      this.cached_entries = cached_entries;
    }
  
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() >= cached_entries;
    }
  }

  private static final short SupportedVersion = 1;
  private static final String FileSignature = "DwarfIdea";

  private static final float kMinLat = -90.0f;
  private static final float kMaxLat = 90.0f;
  private static final float kMinLon = -180.0f;
  private static final float kMaxLon = 180.0f;

  private static final byte kCoordSpecBits = 5;

  FileInputStream db_;
  Stack<Integer> pos_stack_;
  MappedByteBuffer buffer_;
  ByteBuffer tmp_buffer_;
  SliceByteArray tmp_buffer_as_slice_array_, slice_array_, slice_array2_;
  LRUCache<Key, Result> results_cache_;
  LRUCache<Integer, byte[]> keys_cache_;
  LRUCache<Integer, byte[]> coords_cache_;
  LRUCache<Integer, byte[]> extra_data_cache_;
  HashMap<Integer, Short> key_map_;
  FiniteStateEntropy keys_fse_, coords_fse_, extra_data_fse_;
  BWTS bwts_;
  SBRT sbrt_;
  ZRLT zrlt_;
  Key last_key_;
  int key_size_, extra_data_size_, index_offset_, num_entries_, index_size_;
  short min_entries_per_block_, max_entries_per_block_, bounding_box_bits_;
  float max_dist_error_, bounding_box_lat_step_, bounding_box_lon_step_;
}
