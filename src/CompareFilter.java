package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;
import org.hbase.async.generated.ComparatorPB;
import org.hbase.async.generated.FilterPB;
import org.hbase.async.generated.HBasePB;

/**
 * This is a generic filter to be used to filter by comparison. It takes an
 * operator (equal, greater, not equal, etc) and a Comparator.
 */
public abstract class CompareFilter extends ScanFilter {

  /** Comparison operators. */
  public enum CompareOp {
    /** less than */
    LESS,
    /** less than or equal to */
    LESS_OR_EQUAL,
    /** equals */
    EQUAL,
    /** not equal */
    NOT_EQUAL,
    /** greater than or equal to */
    GREATER_OR_EQUAL,
    /** greater than */
    GREATER,
    /** no operation */
    NO_OP,
  }

  final protected CompareOp compareOp;
  final protected Comparator comparator;
  final protected byte[] compareOpBytes;

  public CompareFilter(final CompareOp compareOp, final Comparator comparator) {
    this.compareOp = compareOp;
    this.comparator = comparator;
    this.compareOpBytes = Bytes.UTF8(compareOp.name());
  }

  public org.hbase.async.generated.FilterPB.CompareFilter convert() {
    ComparatorPB.Comparator.Builder comparatorBuilder = ComparatorPB.Comparator
        .newBuilder().setNameBytes(Bytes.wrap(comparator.getName()))
        .setSerializedComparator(Bytes.wrap(comparator.toByteArray()));

    FilterPB.CompareFilter.Builder filter = FilterPB.CompareFilter.newBuilder()
        .setCompareOp(HBasePB.CompareType.valueOf(this.compareOp.name()))
        .setComparator(comparatorBuilder.build());
    return filter.build();
  }

  @Override
  void serializeOld(byte server_version, final ChannelBuffer buf) {
    buf.writeShort(compareOpBytes.length);
    buf.writeBytes(compareOpBytes); // 3 + compareOpBytes.length
    comparator.serializeOld(server_version, buf);
  }

  @Override
  int predictSerializedSize(byte server_version) {
    return 2 + compareOpBytes.length
        + comparator.predictSerializedSize(server_version);
  }

  public String toString() {
    return "compareOp=" + compareOp + " comparator=" + comparator;
  }

}
