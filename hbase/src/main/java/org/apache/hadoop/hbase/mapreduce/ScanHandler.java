package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

public class ScanHandler {
	private static final Log LOG = LogFactory.getLog(TableInputFormat.class);
	
	//Copied from org.apache.hadoop.hbase.mapreduce.TableInputFormat.setConf(Configuration)
	public static Scan getScan(Configuration conf) {
		Scan scan = new Scan();
	    if (conf.get(TableInputFormat.SCAN) != null) {
	      try {
	        scan = TableMapReduceUtil.convertStringToScan(conf.get(TableInputFormat.SCAN));
	      } catch (IOException e) {
	        LOG.error("An error occurred.", e);
	      }
	    } else {
	      try {
	        scan = new Scan();

	        if (conf.get(TableInputFormat.SCAN_COLUMNS) != null) {
	          scan.addColumns(conf.get(TableInputFormat.SCAN_COLUMNS));
	        }

	        if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
	          scan.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
	        }

	        if (conf.get(TableInputFormat.SCAN_TIMESTAMP) != null) {
	          scan.setTimeStamp(Long.parseLong(conf.get(TableInputFormat.SCAN_TIMESTAMP)));
	        }

	        if (conf.get(TableInputFormat.SCAN_TIMERANGE_START) != null && conf.get(TableInputFormat.SCAN_TIMERANGE_END) != null) {
	          scan.setTimeRange(
	              Long.parseLong(conf.get(TableInputFormat.SCAN_TIMERANGE_START)),
	              Long.parseLong(conf.get(TableInputFormat.SCAN_TIMERANGE_END)));
	        }

	        if (conf.get(TableInputFormat.SCAN_MAXVERSIONS) != null) {
	          scan.setMaxVersions(Integer.parseInt(conf.get(TableInputFormat.SCAN_MAXVERSIONS)));
	        }

	        if (conf.get(TableInputFormat.SCAN_CACHEDROWS) != null) {
	          scan.setCaching(Integer.parseInt(conf.get(TableInputFormat.SCAN_CACHEDROWS)));
	        }

	        // false by default, full table scans generate too much BC churn
	        scan.setCacheBlocks((conf.getBoolean(TableInputFormat.SCAN_CACHEBLOCKS, false)));
	      } catch (Exception e) {
	          LOG.error(StringUtils.stringifyException(e));
	      }
	    }
		return scan;
	}
}
