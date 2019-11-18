package com.linkedin.paldb.utils;

import java.util.Arrays;

import static java.lang.Math.log;

public class BloomFilter {
  private static final int BITS_IN_LONG = 64;
  private final long[] bits;
  private final int hashFunctions; // Number of hash functions
  private static final double LN2 = 0.6931471805599453; // ln(2)
  private final int sizeInBits;

  public BloomFilter(int elements, int sizeInBits) {
    this.sizeInBits = sizeInBits;
    this.hashFunctions = Math.max(1, (int) Math.round(LN2 * sizeInBits / elements));
    this.bits = new long[Math.max(1, (int) Math.ceil((double) sizeInBits / BITS_IN_LONG))];
  }

  public BloomFilter(int expectedElements, double errorRate) {
    this.sizeInBits = Math.max(BITS_IN_LONG, (int) Math.ceil( (-1 * expectedElements * log(errorRate)) / (LN2 * LN2)));
    this.hashFunctions = Math.max(1, (int) Math.round(((double) sizeInBits / expectedElements) * LN2));
    this.bits = new long[Math.max(1, (int) Math.ceil((double) sizeInBits / BITS_IN_LONG))];
  }

  public BloomFilter(int hashFunctions, int bitSize, long[] bits) {
    this.sizeInBits = bitSize;
    this.bits = bits;
    this.hashFunctions = hashFunctions;
  }

  public void add(byte[] bytes) {
    for (int i = 0; i < hashFunctions; i++) {
      int value = Math.abs(HashUtils.hash(bytes, i) % sizeInBits);
      setBit(value);
    }
  }

  public boolean mightContain(byte[] bytes) {
    for (int i = 0; i < hashFunctions; i++) {
      int value = Math.abs(HashUtils.hash(bytes, i) % sizeInBits);
      if (!getBit(value)) return false;
    }
    return true;
  }

  public long[] bits() {
    return bits;
  }

  private void setBit(int position) {
    int flagIndex = position / BITS_IN_LONG;
    int bitIndexInFlag = position % BITS_IN_LONG;
    bits[flagIndex] |= (1L << bitIndexInFlag);
  }

  private boolean getBit(int position) {
    int flagIndex = position / BITS_IN_LONG;
    int bitIndexInFlag = position % BITS_IN_LONG;
    return ((bits[flagIndex] >> bitIndexInFlag) & 1L) == 1;
  }

  public void clear() {
    for (int i = 0; i < bits.length; i++) {
      bits[i] = 0L;
    }
  }

  public int bitSize() {
    return sizeInBits;
  }

  public int hashFunctions() {
    return hashFunctions;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bits) ^ hashFunctions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BloomFilter)) return false;
    final var that = (BloomFilter) o;
    return Arrays.equals(this.bits, that.bits) && this.hashFunctions == that.hashFunctions;
  }

  public void merge(BloomFilter other) {
    if (other.hashFunctions != this.hashFunctions || other.bits.length != this.bits.length) {
      throw new IllegalArgumentException("Incompatible bloom filters");
    }

    for (int i = 0; i < bits.length; i++) {
      var oldBit = getBit(i);
      if (!oldBit && other.getBit(i)) {
        setBit(i);
      }
    }
  }
}

