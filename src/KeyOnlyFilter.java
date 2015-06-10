package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;

import org.hbase.async.generated.FilterPB;

public final class KeyOnlyFilter extends ScanFilter {

  private static final byte[] NAME = Bytes.ISO88591("org.apache.hadoop"
      + ".hbase.filter.KeyOnlyFilter");

  private final boolean lenAsVal;

  public KeyOnlyFilter(final boolean lenAsVal) {
    this.lenAsVal = lenAsVal;
  }

  public KeyOnlyFilter() {
    this(false);
  }

  @Override
  byte[] serialize() {
    return FilterPB.KeyOnlyFilter.newBuilder().setLenAsVal(lenAsVal).build()
        .toByteArray();
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  int predictSerializedSize() {
    return 1 + NAME.length + 1;
  }

  @Override
  void serializeOld(final ChannelBuffer buf) {
    buf.writeByte((byte) NAME.length); // 1
    buf.writeBytes(NAME);
    buf.writeByte(lenAsVal ? 1 : 0); // 1
  }

  public String toString() {
    return "KeyOnlyFilter(" + lenAsVal + ")";
  }

}
