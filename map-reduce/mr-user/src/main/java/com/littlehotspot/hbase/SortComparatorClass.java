package com.littlehotspot.hbase;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *@Author 刘飞飞
 *@Date 2017/7/20 14:10
 */
public class SortComparatorClass extends WritableComparator {
    public SortComparatorClass () {
          super(SortKeyPair.class, true);
    }
      @Override
      public int compare(WritableComparable a, WritableComparable b) {
            SortKeyPair s1 = (SortKeyPair)a;
            SortKeyPair s2 = (SortKeyPair)b;
            return s1.compareTo(s2);
      }

}
