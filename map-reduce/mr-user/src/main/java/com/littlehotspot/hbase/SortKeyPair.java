package com.littlehotspot.hbase;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *@Author 刘飞飞
 *@Date 2017/7/20 13:59
 */
@Data
public class SortKeyPair implements WritableComparable<SortKeyPair> {
    private int readCount = 0;
    public SortKeyPair() {}
    public SortKeyPair(int readCount) {
        super();
        this.readCount= readCount;
    }
    @Override
    public int compareTo(SortKeyPair o) {
        int res = this.readCount < o.getReadCount() ? 1: (this.readCount == o.getReadCount() ? 0 : -1);
        return res;
    }

    @Override
    public void write(DataOutput out) throws IOException {
          out.writeInt(readCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
          this.readCount=in.readInt();
    }

      @Override
      public int hashCode() {
         return Integer.MAX_VALUE-this.readCount;
      }

      @Override
      public String toString() {
           return this.readCount+"";
      }
}
