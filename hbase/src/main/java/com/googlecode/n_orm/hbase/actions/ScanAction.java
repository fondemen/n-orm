package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
//import org.apache.hadoop.hbase.client.Scan;
import org.hbase.async.Scanner;

import com.googlecode.n_orm.hbase.MangledTableName;
import com.stumbleupon.async.Deferred;


public class ScanAction extends Action</*Result*/Scanner> {
       
        private final Scanner scan;
        private final MangledTableName tableName;

        public ScanAction(Scanner scan, MangledTableName tableName) {
                super();
                this.scan = scan;
                this.tableName=tableName;
        }

        public Scanner getScan() {
                return scan;
        }
        
        public MangledTableName getMangledTableName() {
            return tableName;
    }

		@Override
		public Deferred<Scanner> perform(HBaseClient client) throws Exception {
			client.newScanner(this.getMangledTableName().getNameAsBytes());
			return null;
		}
       
}
