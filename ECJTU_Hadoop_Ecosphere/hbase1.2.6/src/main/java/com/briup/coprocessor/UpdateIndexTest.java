package com.briup.coprocessor;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class UpdateIndexTest
    extends BaseRegionObserver {

    Table table = null;
    PrintWriter out = null;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        out = new PrintWriter("/home/client/follower.log");
        //拿到明星表 操作DML的句柄对象
        table = e.getTable(TableName.valueOf("bd1902:followed"));
        out.println("start");
        out.flush();
    }

    //监听的行为是事件源(粉丝表)插入数据之后
    // arg1 上下文对象
    // arg2 触发postPut的put行为细节
    @Override
    public void postPut
        (ObserverContext<RegionCoprocessorEnvironment> c,
            Put put, WALEdit edit, Durability durability) throws IOException {
        //获取粉丝id所在的cell
        Cell fan = put.get
            (Bytes.toBytes("f"),
                Bytes.toBytes("from")).get(0);
        //获取明星id所在的cell
        Cell star = put.get
            (Bytes.toBytes("f"),
                Bytes.toBytes("to")).get(0);
        //从cell去取值 取得粉丝id
        String fan_id = Bytes.toString
            (fan.getValueArray(),
                fan.getValueOffset(),
                    fan.getValueLength());
        //从cell去取值 取得明星id
        String star_id =
            Bytes.toString(
                star.getValueArray(),
                    star.getValueOffset(),
                    star.getValueLength());

        //构建新的put插入数据到明星表
        Put p = new Put
            (Bytes.toBytes(star_id+","+fan_id));
        out.println("postPut获取到id 50:"+star_id+","+fan_id);
        out.flush();

        //从follower中获取除了rowkey以外的数据
        //放入新的put中 插入到followed
        NavigableMap<byte[], List<Cell>>
            familyCellMap = put.getFamilyCellMap();
        for (Map.Entry<byte[], List<Cell>> entry
                : familyCellMap.entrySet()) {
            String k = Bytes.toString(entry.getKey());
            for (Cell cell : entry.getValue()) {
                out.println("postPut获取到id 56:"+star_id+","+fan_id);
                out.flush();
                p.addColumn(Bytes.copy(cell.getFamilyArray(),
                    cell.getFamilyOffset(),cell.getFamilyLength()),
                    Bytes.copy(cell.getQualifierArray(),
                        cell.getQualifierOffset(),cell.getQualifierLength()),
                    Bytes.copy(cell.getValueArray(),
                        cell.getValueOffset(),cell.getValueLength()));
            }
        }


        out.println("postPut获取到id 57:"+star_id+","+fan_id);
        out.flush();

        table.put(p);
        table.close();

        out.println("postPut获取到id 61:"+star_id+","+fan_id);
        out.flush();
        out.println("插入完毕:"+star_id+","+fan_id);
        out.flush();
    }


    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> c,
                          Get get, List<Cell> result) {
        byte[] row = get.getRow();
        String key = Bytes.toString(row);
        if(key.endsWith("f1,s1")){
            Cell kv = new KeyValue(row,Bytes.toBytes("test"),
                    Bytes.toBytes("test"),Bytes.toBytes("coprotest"));
            result.add(kv);
        }
        out.println("postGetOp");
        out.flush();

    }
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        table.close();
        out.println("stop");
        out.flush();
        out.close();
    }
}



    //获取粉丝id
//    Cell fan = put.get(Bytes.toBytes("f"),
//            Bytes.toBytes("from")).get(0);
//    Cell star = put.get(Bytes.toBytes("f"),
//            Bytes.toBytes("to")).get(0);
//
//    String fan_id = Bytes.toString(fan.getValueArray());
//    String star_id = Bytes.toString(star.getValueArray());
//
//    Table table = ConnectionFactory.createConnection
//            (c.getEnvironment().getConfiguration())
//            .getTable(TableName.valueOf("IMUT:followed"));
//
//    Put np = new Put(Bytes.toBytes(star_id+","+fan_id));
//    NavigableMap<byte[], List<Cell>> familyCellMap
//            = put.getFamilyCellMap();
//        for (Map.Entry<byte[], List<Cell>> entry
//        : familyCellMap.entrySet()) {
//        List<Cell> list = entry.getValue();
//        byte[] f = entry.getKey();
//        for (Cell l : list) {
//        np.addColumn(l.getFamilyArray(),
//        l.getQualifierArray(),l.getValueArray());
//        }
//        }
//        table.put(np);