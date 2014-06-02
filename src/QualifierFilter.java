package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;
import org.hbase.async.generated.FilterPB;

/**
 * This filter is used to filter based on the column qualifier. It takes an
 * operator (equal, greater, not equal, etc) and a byte [] comparator for the
 * column qualifier portion of a key.
 */
public final class QualifierFilter extends CompareFilter {
  private static final byte[] NAME = Bytes
      .ISO88591("org.apache.hadoop.hbase.filter.QualifierFilter");

  public static byte writableByteArrayComparableCode(byte server_version) {
    // position in array org.apache.hadoop.hbase.io.HbaseObjectWritable
    if (server_version <= RegionClient.SERVER_VERSION_092_OR_ABOVE) {
      return 0x35;
    } else {
      return 0x34;
    }
  }

  /**
   * Constructor.
   *
   * @param qualifierCompareOp
   *          the compare op for column qualifier matching
   * @param qualifierComparator
   *          the comparator for column qualifier matching
   */
  public QualifierFilter(final CompareOp qualifierCompareOp,
      final Comparator qualifierComparator) {
    super(qualifierCompareOp, qualifierComparator);
  }

  @Override
  byte[] serialize() {
    final FilterPB.QualifierFilter.Builder filter = FilterPB.QualifierFilter
        .newBuilder();
    filter.setCompareFilter(super.convert());
    return filter.build().toByteArray();
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  int predictSerializedSize(byte server_version) {
    return 1 + NAME.length + super.predictSerializedSize(server_version);
  }

  @Override
  void serializeOld(byte server_version, final ChannelBuffer buf) {
    buf.writeByte((byte) NAME.length); // 1
    buf.writeBytes(NAME);
    super.serializeOld(server_version, buf);
  }

  public String toString() {
    return "QualifierFilter(" + super.toString() + ")";
  }

}
