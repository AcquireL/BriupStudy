package com.briup.grms.step5;

import org.apache.hadoop.mapreduce.Partitioner;

import javax.xml.soap.Text;

/**
 * 分区比较器
 */
public class IdFlagPartitioner extends Partitioner<IdFlag, Text> {
    @Override
    public int getPartition(IdFlag idFlag, Text text, int numPartitions) {
        return Math.abs (idFlag.getGid ().toString ().hashCode ())%127;
    }
}
