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
       
        private final Scan scan;
        private MangledTableName tableName;

        public ScanAction(Scan s, MangledTableName tableName) {
                super();
                this.scan = s;
                this.tableName=tableName;
        }

		public Scan getScan() {
                return scan;
        }
        
        public MangledTableName getMangledTableName() {
            return tableName;
    }

		@Override
		public Deferred<Scanner> perform(HBaseClient client) throws Exception {
			client.newScanner(this.getMangledTableName().getNameAsBytes()); // to create a new scanner for a particular table
			 														  
			return null;
		}
		
		@Override
		public MangledTableName getTable() {
			return tableName;
		}
		
		public void setTable(MangledTableName table){
			this.tableName=table;
		}
       
}
