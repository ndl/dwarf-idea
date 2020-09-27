/*
 * The code is based on https://github.com/airlift/aircompressor/blob/master/src/main/java/io/airlift/compress/zstd/BitInputStream.java
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.endl.dwarf_idea;

import java.nio.ByteBuffer;

import static ch.endl.dwarf_idea.Util.highestBit;
import static ch.endl.dwarf_idea.Util.verify;

/**
 * Bit streams are encoded as a byte-aligned little-endian stream. Thus, bits are laid out
 * in the following manner, and the stream is read from right to left.
 * <p>
 * <p>
 * ... [16 17 18 19 20 21 22 23] [8 9 10 11 12 13 14 15] [0 1 2 3 4 5 6 7]
 */
class BitStream
{
    private static final int SIZE_OF_LONG = 8;

    private BitStream()
    {
    }

    public static boolean isEndOfStream(int startOffset, int currentOffset, int bitsConsumed)
    {
        return startOffset == currentOffset && bitsConsumed == Long.SIZE;
    }

    static long readTail(ByteBuffer buffer, int inputOffset, int inputSize)
    {
        long bits = buffer.get(inputOffset) & 0xFF;

        switch (inputSize) {
            case 7:
                bits |= (buffer.get(inputOffset + 6) & 0xFFL) << 48;
            case 6:
                bits |= (buffer.get(inputOffset + 5) & 0xFFL) << 40;
            case 5:
                bits |= (buffer.get(inputOffset + 4) & 0xFFL) << 32;
            case 4:
                bits |= (buffer.get(inputOffset + 3) & 0xFFL) << 24;
            case 3:
                bits |= (buffer.get(inputOffset + 2) & 0xFFL) << 16;
            case 2:
                bits |= (buffer.get(inputOffset + 1) & 0xFFL) << 8;
        }

        return bits;
    }

    /**
     * @return numberOfBits in the low order bits of a long
     */
    public static long peekBits(int bitsConsumed, long bitContainer, int numberOfBits)
    {
        return (((bitContainer << bitsConsumed) >>> 1) >>> (63 - numberOfBits));
    }

    /**
     * numberOfBits must be > 0
     *
     * @return numberOfBits in the low order bits of a long
     */
    public static long peekBitsFast(int bitsConsumed, long bitContainer, int numberOfBits)
    {
        return ((bitContainer << bitsConsumed) >>> (64 - numberOfBits));
    }

    static class Initializer
    {
        private final ByteBuffer buffer;
        private final int startOffset;
        private final int endOffset;
        private long bits;
        private int currentOffset;
        private int bitsConsumed;

        public Initializer(ByteBuffer buffer, int startOffset, int endOffset)
        {
            this.buffer = buffer;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        public long getBits()
        {
            return bits;
        }

        public int getCurrentOffset()
        {
            return currentOffset;
        }

        public int getBitsConsumed()
        {
            return bitsConsumed;
        }

        public void initialize()
        {
            verify(endOffset - startOffset >= 1, startOffset, "Bitstream is empty");

            int lastByte = buffer.get(endOffset - 1) & 0xFF;
            verify(lastByte != 0, endOffset, "Bitstream end mark not present");

            bitsConsumed = SIZE_OF_LONG - highestBit(lastByte);

            int inputSize = endOffset - startOffset;
            if (inputSize >= SIZE_OF_LONG) {  /* normal case */
                currentOffset = endOffset - SIZE_OF_LONG;
                bits = buffer.getLong(currentOffset);
            }
            else {
                currentOffset = startOffset;
                bits = readTail(buffer, startOffset, inputSize);

                bitsConsumed += (SIZE_OF_LONG - inputSize) * 8;
            }
        }
    }

    static final class Loader
    {
        private final ByteBuffer buffer;
        private final int startOffset;
        private long bits;
        private int currentOffset;
        private int bitsConsumed;
        private boolean overflow;

        public Loader(ByteBuffer buffer, int startOffset, int currentOffset, long bits, int bitsConsumed)
        {
            this.buffer = buffer;
            this.startOffset = startOffset;
            this.bits = bits;
            this.currentOffset = currentOffset;
            this.bitsConsumed = bitsConsumed;
        }

        public long getBits()
        {
            return bits;
        }

        public int getCurrentOffset()
        {
            return currentOffset;
        }

        public int getBitsConsumed()
        {
            return bitsConsumed;
        }

        public boolean isOverflow()
        {
            return overflow;
        }

        public boolean load()
        {
            if (bitsConsumed > 64) {
                overflow = true;
                return true;
            }

            else if (currentOffset == startOffset) {
                return true;
            }

            int bytes = bitsConsumed >>> 3; // divide by 8
            if (currentOffset >= startOffset + SIZE_OF_LONG) {
                if (bytes > 0) {
                    currentOffset -= bytes;
                    bits = buffer.getLong(currentOffset);
                }
                bitsConsumed &= 0b111;
            }
            else if (currentOffset - bytes < startOffset) {
                bytes = currentOffset - startOffset;
                currentOffset = startOffset;
                bitsConsumed -= bytes * SIZE_OF_LONG;
                bits = buffer.getLong(startOffset);
                return true;
            }
            else {
                currentOffset -= bytes;
                bitsConsumed -= bytes * SIZE_OF_LONG;
                bits = buffer.getLong(currentOffset);
            }

            return false;
        }
    }
}
