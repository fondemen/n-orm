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
			final Scanner res=client.newScanner(this.getMangledTableName().getNameAsBytes()); // to create a new scanner for a particular table
			byte[] startKey = this.getScan().getStartRow();
			byte[] stopKey = this.getScan().getStopRow();
			byte[] family=this.getScan().getFamily();
			byte[] qualifier=this.getScan().getQualifier();
			byte[][] qualifiers=this.getScan().getQualifiers();
			if(startKey!=null){
				res.setStartKey(startKey);
			}
			if(stopKey!=null){
				res.setStopKey(stopKey);
			}
			if(family!=null){
				res.setFamily(family);
			}
			if(qualifier!=null){
				res.setQualifier(qualifier);
			}
			if(qualifiers!=null){
				res.setQualifiers(qualifiers);
			}	
			return Deferred.fromResult(res);
		}
		
		@Override
		public MangledTableName getTable() {
			return tableName;
		}
		
		public void setTable(MangledTableName table){
			this.tableName=table;
		}
       
}
