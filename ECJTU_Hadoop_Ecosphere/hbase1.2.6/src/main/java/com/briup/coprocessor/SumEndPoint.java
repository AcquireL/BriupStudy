package com.briup.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SumEndPoint extends Sum.SumService
        implements Coprocessor,CoprocessorService {
    private RegionCoprocessorEnvironment env;
    @Override
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        this.env =
            (RegionCoprocessorEnvironment)
                coprocessorEnvironment;
    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {

    }

    @Override
    public Service getService() {
        return this;
    }

    //Servlet doPost(context,request,response)
    @Override
    public void getSum(RpcController controller,
       Sum.SumRequest request,
           RpcCallback<Sum.SumResponse> done) {
        Sum.SumResponse res = null;

        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes(request.getFamily()));
//        scan.addColumn(Bytes.toBytes(request.getFamily()),
//                Bytes.toBytes(request.getColumn()));
        long sum = 0;
        try {
            RegionScanner rscanner =
                env.getRegion().getScanner(scan);
            List<Cell> cells = new ArrayList<>();
            while(rscanner.next(cells)){
                sum++;
            }
            res = Sum.SumResponse.newBuilder().setSum(sum).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
        done.run(res);
    }
}
