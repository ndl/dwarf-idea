/*
 * The code is based on https://github.com/airlift/aircompressor/blob/master/src/main/java/io/airlift/compress/zstd/FiniteStateEntropy.java
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

import static ch.endl.dwarf_idea.BitStream.peekBits;
import static ch.endl.dwarf_idea.FseTableReader.FSE_MAX_SYMBOL_VALUE;
import static ch.endl.dwarf_idea.Util.verify;

class FiniteStateEntropy
{
    private static final int MAX_TABLE_LOG = 12;
    private static final int SIZE_OF_INT = 4;

    private final FiniteStateEntropy.Table table;
    private final FseTableReader reader = new FseTableReader();

    public FiniteStateEntropy(final ByteBuffer inputBuffer, final int inputOffset, final int inputSize)
    {
        table = new FiniteStateEntropy.Table(MAX_TABLE_LOG);
        reader.readFseTable(table, inputBuffer, inputOffset, inputOffset + inputSize, FSE_MAX_SYMBOL_VALUE, MAX_TABLE_LOG);
    }

    public int decompress(final ByteBuffer inputBuffer, final int inputOffset, final int inputSize, ByteBuffer outputBuffer)
    {
        int input = inputOffset;
        int inputLimit = input + inputSize;

        // initialize bit stream
        BitStream.Initializer initializer = new BitStream.Initializer(inputBuffer, input, inputLimit);
        initializer.initialize();
        int bitsConsumed = initializer.getBitsConsumed();
        int currentOffset = initializer.getCurrentOffset();
        long bits = initializer.getBits();

        // initialize first FSE stream
        int state1 = (int) peekBits(bitsConsumed, bits, table.log2Size);
        bitsConsumed += table.log2Size;

        BitStream.Loader loader = new BitStream.Loader(inputBuffer, input, currentOffset, bits, bitsConsumed);
        loader.load();
        bits = loader.getBits();
        bitsConsumed = loader.getBitsConsumed();
        currentOffset = loader.getCurrentOffset();

        // initialize second FSE stream
        int state2 = (int) peekBits(bitsConsumed, bits, table.log2Size);
        bitsConsumed += table.log2Size;

        loader = new BitStream.Loader(inputBuffer, input, currentOffset, bits, bitsConsumed);
        loader.load();
        bits = loader.getBits();
        bitsConsumed = loader.getBitsConsumed();
        currentOffset = loader.getCurrentOffset();

        byte[] symbols = table.symbol;
        byte[] numbersOfBits = table.numberOfBits;
        int[] newStates = table.newState;

        int output = 0;
        final int outputLimit = outputBuffer.capacity();

        // decode 2 symbols per loop
        while (output <= outputLimit - 2) {
            int numberOfBits;

            outputBuffer.put(output, symbols[state1]);
            numberOfBits = numbersOfBits[state1];
            state1 = (int) (newStates[state1] + peekBits(bitsConsumed, bits, numberOfBits));
            bitsConsumed += numberOfBits;

            outputBuffer.put(output + 1, symbols[state2]);
            numberOfBits = numbersOfBits[state2];
            state2 = (int) (newStates[state2] + peekBits(bitsConsumed, bits, numberOfBits));
            bitsConsumed += numberOfBits;

            output += 2;

            loader = new BitStream.Loader(inputBuffer, input, currentOffset, bits, bitsConsumed);
            boolean done = loader.load();
            bitsConsumed = loader.getBitsConsumed();
            bits = loader.getBits();
            currentOffset = loader.getCurrentOffset();
            if (done) {
                break;
            }
        }

        while (true) {
            verify(output <= outputLimit - 2, input, "Output buffer is too small");
            outputBuffer.put(output++, symbols[state1]);
            int numberOfBits = numbersOfBits[state1];
            state1 = (int) (newStates[state1] + peekBits(bitsConsumed, bits, numberOfBits));
            bitsConsumed += numberOfBits;

            loader = new BitStream.Loader(inputBuffer, input, currentOffset, bits, bitsConsumed);
            loader.load();
            bitsConsumed = loader.getBitsConsumed();
            bits = loader.getBits();
            currentOffset = loader.getCurrentOffset();

            if (loader.isOverflow()) {
                outputBuffer.put(output++, symbols[state2]);
                break;
            }

            verify(output <= outputLimit - 2, input, "Output buffer is too small");
            outputBuffer.put(output++, symbols[state2]);
            int numberOfBits1 = numbersOfBits[state2];
            state2 = (int) (newStates[state2] + peekBits(bitsConsumed, bits, numberOfBits1));
            bitsConsumed += numberOfBits1;

            loader = new BitStream.Loader(inputBuffer, input, currentOffset, bits, bitsConsumed);
            loader.load();
            bitsConsumed = loader.getBitsConsumed();
            bits = loader.getBits();
            currentOffset = loader.getCurrentOffset();

            if (loader.isOverflow()) {
                outputBuffer.put(output++, symbols[state1]);
                break;
            }
        }

        return output;
    }

    public static final class Table
    {
        int log2Size;
        final int[] newState;
        final byte[] symbol;
        final byte[] numberOfBits;

        public Table(int log2Size)
        {
            int size = 1 << log2Size;
            newState = new int[size];
            symbol = new byte[size];
            numberOfBits = new byte[size];
        }

        public Table(int log2Size, int[] newState, byte[] symbol, byte[] numberOfBits)
        {
            int size = 1 << log2Size;
            if (newState.length != size || symbol.length != size || numberOfBits.length != size) {
                throw new IllegalArgumentException("Expected arrays to match provided size");
            }

            this.log2Size = log2Size;
            this.newState = newState;
            this.symbol = symbol;
            this.numberOfBits = numberOfBits;
        }
    }
}
