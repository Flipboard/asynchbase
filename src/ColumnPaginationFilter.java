package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;
import org.hbase.async.generated.FilterPB;

/**
 * A filter, based on the ColumnCountGetFilter, takes two arguments: limit and
 * offset. This filter can be used for row-based indexing, where references to
 * other tables are stored across many columns, in order to efficient lookups
 * and paginated results for end users. Only most recent versions are considered
 * for pagination.
 */
public final class ColumnPaginationFilter extends ScanFilter {

  private static final byte[] NAME = Bytes
      .ISO88591("org.apache.hadoop.hbase.filter.ColumnPaginationFilter");

  private int limit = 0;
  private int offset = -1;
  private byte[] columnOffset = null; // only hbase >= 0.96

  /**
   * Initializes filter with an integer offset and limit. The offset is arrived
   * at scanning sequentially and skipping entries. @limit number of columns are
   * then retrieved. If multiple column families are involved, the columns may
   * be spread across them.
   *
   * @param limit
   *          Max number of columns to return.
   * @param offset
   *          The integer offset where to start pagination.
   */
  public ColumnPaginationFilter(final int limit, final int offset) {
    this.limit = limit;
    this.offset = offset;
  }

  /**
   * Initializes filter with a string/bookmark based offset and limit. The
   * offset is arrived at, by seeking to it using scanner hints. If multiple
   * column families are involved, pagination starts at the first column family
   * which contains @columnOffset. Columns are then retrieved sequentially upto @limit
   * number of columns which maybe spread across multiple column families,
   * depending on how the scan is setup.
   *
   * @param limit
   *          Max number of columns to return.
   * @param columnOffset
   *          The string/bookmark offset on where to start pagination.
   * @since hbase >= 0.96
   */
  public ColumnPaginationFilter(final int limit, final byte[] columnOffset) {
    this.limit = limit;
    this.columnOffset = columnOffset;
  }

  @Override
  byte[] serialize() {
    final FilterPB.ColumnPaginationFilter.Builder filter = FilterPB.ColumnPaginationFilter
        .newBuilder();

    filter.setLimit(this.limit);
    if (this.offset >= 0) {
      filter.setOffset(this.offset);
    }

    if (this.columnOffset != null) {
      filter.setColumnOffset(Bytes.wrap(this.columnOffset));
    }

    return filter.build().toByteArray();
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  int predictSerializedSize(byte server_version) {
    return 1 + NAME.length + 4 + 4;
  }

  @Override
  void serializeOld(byte server_version, final ChannelBuffer buf) {
    buf.writeByte((byte) NAME.length); // 1
    buf.writeBytes(NAME); // 48

    buf.writeInt(this.limit); // 4: int
    buf.writeInt(this.offset); // 4: int

    // because versions < 0.96 don't support columnOffset
    // we don't serialize here
  }

  public String toString() {
    return "ColumnPaginationFilter(limit=" + limit + " offset=" + offset
        + " columnOffset=" + Bytes.pretty(columnOffset) + ")";
  }

}
