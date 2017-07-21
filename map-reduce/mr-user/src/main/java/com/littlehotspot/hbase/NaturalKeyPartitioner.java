package com.littlehotspot.hbase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *@Author 刘飞飞
 *@Date 2017/7/20 14:12
 */
public class NaturalKeyPartitioner extends Partitioner<SortKeyPair,Text> {
    @Override
    public int getPartition(SortKeyPair sortKeyPair, Text text, int numPartitions) {
          int count =sortKeyPair.getReadCount();
          if (count <= 0) {
              return 0;
          } else if (count > 0 && count <= 100) {
              return 1;
          } else if (count > 100 && count <= 200) {
              return 2;
          } else {
              return 3;
          }
    }
}
