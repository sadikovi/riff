package com.github.sadikovi.serde;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * [[BitArray]] class is used to set individual bits similar to bit set, but with forced size, and
 * friendly write methods. Values are stored in little endian (bits order is right to left, so
 * position is read from right to left).
 */
public class BitArray {
  private static final int OCTETS = 8;
  private byte[] bits;

  public BitArray(int numBits) {
    if (numBits < 0) {
      throw new IllegalArgumentException("Negative number of bits requested, " + numBits);
    }
    numBits = numBits / OCTETS + ((numBits % OCTETS == 0) ? 0 : 1);
    this.bits = new byte[numBits];
  }

  /** Number of allocated bytes in array */
  public int length() {
    return this.bits.length;
  }

  /**
   * Set bit at position, throws exception is position is invalid.
   */
  public void setBit(int position) {
    int bytePos = position / OCTETS;
    this.bits[bytePos] |= 1 << (position % OCTETS);
  }

  /**
   * Clear bit at position, throws exception if position is invalid.
   */
  public void clearBit(int position) {
    int bytePos = position / OCTETS;
    this.bits[bytePos] &= ~(1 << (position % OCTETS));
  }

  /**
   * Return true if bit at position is set to 1. If bit is 0 return false.
   * Also if position is invalid or larger than array return false - this is done for serialized
   * version being read and having fewer bytes.
   */
  public boolean isSet(int position) {
    int bytePos = position / OCTETS;
    if (bytePos < 0 || bytePos >= this.bits.length) return false;
    return (this.bits[bytePos] & (1 << (position % OCTETS))) != 0;
  }

  /**
   * Whether or not no bits are set.
   */
  public boolean isEmpty() {
    for (int i = 0; i < this.bits.length; i++) {
      if (this.bits[i] != 0) return false;
    }
    return true;
  }

  /**
   * Clear and reset all bits in this array.
   */
  public void clear() {
    for (int i = 0; i < this.bits.length; i++) {
      this.bits[i] = 0;
    }
  }

  /**
   * Write current bit array into provided output stream.
   * Since we write from left to right, we can drop any right-most empty bits - written length is
   * adjusted to written bytes.
   */
  public void writeTo(DataOutputStream out) throws IOException {
    // find right most byte that should be written
    int right = this.bits.length - 1;
    while (right >= 0 && this.bits[right] == 0) right--;
    // write length, use 0 if right = -1
    out.writeInt(right + 1);
    int i = 0;
    while (i <= right) {
      out.writeByte(this.bits[i]);
      ++i;
    }
  }

  /**
   * Read from stream, current instance is cleared and updated to use serialized version of bit
   * array. Note that length is also adjusted and might be different from bit array length that
   * was used for write.
   */
  public void readFrom(DataInputStream in) throws IOException {
    // read length
    int len = in.readInt();
    this.bits = new byte[len];
    int i = 0;
    while (i < len) {
      this.bits[i] = in.readByte();
      ++i;
    }
  }

  @Override
  public boolean equals(Object other) {
    // never matches other objects
    return false;
  }

  @Override
  public int hashCode() {
    return 31 * this.bits.length;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("bits[");
    // LSB is left-most byte, see class description
    for (int i = 0; i < this.bits.length; i++) {
      sb.append(Integer.toBinaryString(this.bits[i] & 0xff));
      if (i < this.bits.length - 1) {
        sb.append("|");
      }
    }
    sb.append("]");
    return sb.toString();
  }
}
