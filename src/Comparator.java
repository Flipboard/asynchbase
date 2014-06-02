package org.hbase.async;

import org.hbase.async.generated.ComparatorPB;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.util.CharsetUtil;

import java.nio.charset.Charset;

/** Base class for byte array comparators */
public abstract class Comparator {

  abstract public byte[] getName();

  abstract public byte[] toByteArray();

  abstract public void serializeOld(final byte server_version,
      final ChannelBuffer buf);

  abstract int predictSerializedSize(byte server_version);

  /** Matches against a Regular Expression */
  public static class RegexStringComparator extends Comparator {
    private static final byte[] NAME = Bytes
        .ISO88591("org.apache.hadoop.hbase.filter.RegexStringComparator");
    private byte[] charset;
    private byte[] pattern;

    public RegexStringComparator(final String regexp) {
      this(regexp, CharsetUtil.ISO_8859_1);
    }

    public RegexStringComparator(final String pattern, final Charset charset) {
      this.pattern = Bytes.UTF8(pattern);
      this.charset = Bytes.UTF8(charset.name());
    }

    @Override
    final public byte[] getName() {
      return NAME;
    }

    @Override
    public byte[] toByteArray() {
      return ComparatorPB.RegexStringComparator.newBuilder()
          .setPatternBytes(Bytes.wrap(pattern)).setPatternFlags(0) // ?
          .setCharsetBytes(Bytes.wrap(charset)).build().toByteArray();
    }

    @Override
    public void serializeOld(final byte server_version, final ChannelBuffer buf) {
      buf.writeByte(ScanFilter.writableByteArrayComparableCode(server_version)); // Code
                                                                                 // for
                                                                                 // Writable
      buf.writeByte(0); // Code for "this has no code". // 1
      buf.writeByte((byte) getName().length);
      buf.writeBytes(getName());
      buf.writeShort(pattern.length); // + 2
      buf.writeBytes(pattern);
      buf.writeShort(charset.length); // + 2
      buf.writeBytes(charset);
    }

    @Override
    int predictSerializedSize(byte server_version) {
      return 1 + 1 + 1 + getName().length + 2 + pattern.length + 2
          + charset.length;
    }

  }

  /** Lexicographically compares against the specified byte array */
  public static class BinaryComparator extends Comparator {
    private static final byte[] NAME = Bytes
        .ISO88591("org.apache.hadoop.hbase.filter.BinaryComparator");

    protected final byte[] value;

    public BinaryComparator(byte[] value) {
      this.value = value;
    }

    @Override
    final public byte[] getName() {
      return NAME;
    }

    @Override
    public byte[] toByteArray() {
      return ComparatorPB.ByteArrayComparable.newBuilder()
          .setValue(Bytes.wrap(this.value)).build().toByteArray();
    }

    @Override
    public void serializeOld(final byte server_version, final ChannelBuffer buf) {
      buf.writeByte(ScanFilter.writableByteArrayComparableCode(server_version)); // Code
                                                                                 // for
                                                                                 // Writable
      buf.writeByte(0); // Code for "this has no code". // 1
      buf.writeByte((byte) getName().length);
      buf.writeBytes(getName());
      HBaseRpc.writeByteArray(buf, value);
    }

    @Override
    int predictSerializedSize(byte server_version) {
      return 1 + 1 + 1 + getName().length + 3 + value.length;
    }
  }
}
