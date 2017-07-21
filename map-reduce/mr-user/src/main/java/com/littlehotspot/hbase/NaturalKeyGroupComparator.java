package com.littlehotspot.hbase;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *@Author 刘飞飞
 *@Date 2017/7/20 14:21
 */
public class NaturalKeyGroupComparator extends WritableComparator {
    public NaturalKeyGroupComparator() {
        super(SortKeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        SortKeyPair s1 = (SortKeyPair) a;
        SortKeyPair s2 = (SortKeyPair) b;
        int res = s1.getReadCount() < s2.getReadCount() ? 1 : (s1.getReadCount() == s2
                .getReadCount() ? 0 : -1);
        return res;
    }
}