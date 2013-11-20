package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Scan;
import org.hbase.async.Scanner;

import com.stumbleupon.async.Deferred;


public class ScanAction extends Action</*Result*/Scanner> {
       
        private final Scanner scan;

        public ScanAction(Scanner scan) {
                super();
                this.scan = scan;
        }

        public Scanner getScan() {
                return scan;
        }

        @Override
        public Deferred<Scanner> perform() throws IOException {
                return this.getClient().newScanner(this.getScan());
        }
       
}
