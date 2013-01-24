package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.hbase.mapreduce.TableInputFormat.*;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

public class ScanHandler {
	private static final Log LOG = LogFactory.getLog(TableInputFormat.class);

	// Copied from
	// org.apache.hadoop.hbase.mapreduce.TableInputFormat.setConf(Configuration)
	public static Scan getScan(Configuration conf) {
		Scan scan = new Scan();
		if (conf.get(SCAN) != null) {
			try {
				scan = TableMapReduceUtil.convertStringToScan(conf.get(SCAN));
			} catch (IOException e) {
				LOG.error("An error occurred.", e);
			}
		} else {
			try {
				scan = new Scan();

				if (conf.get(SCAN_COLUMNS) != null) {
					addColumns(scan, conf.get(SCAN_COLUMNS));
				}

				if (conf.get(SCAN_COLUMN_FAMILY) != null) {
					scan.addFamily(Bytes.toBytes(conf.get(SCAN_COLUMN_FAMILY)));
				}

				if (conf.get(SCAN_TIMESTAMP) != null) {
					scan.setTimeStamp(Long.parseLong(conf.get(SCAN_TIMESTAMP)));
				}

				if (conf.get(SCAN_TIMERANGE_START) != null
						&& conf.get(SCAN_TIMERANGE_END) != null) {
					scan.setTimeRange(
							Long.parseLong(conf.get(SCAN_TIMERANGE_START)),
							Long.parseLong(conf.get(SCAN_TIMERANGE_END)));
				}

				if (conf.get(SCAN_MAXVERSIONS) != null) {
					scan.setMaxVersions(Integer.parseInt(conf
							.get(SCAN_MAXVERSIONS)));
				}

				if (conf.get(SCAN_CACHEDROWS) != null) {
					scan.setCaching(Integer.parseInt(conf.get(SCAN_CACHEDROWS)));
				}

				// false by default, full table scans generate too much BC churn
				scan.setCacheBlocks((conf.getBoolean(SCAN_CACHEBLOCKS, false)));
			} catch (Exception e) {
				LOG.error(StringUtils.stringifyException(e));
			}
		}
		return scan;
	}

	private static void addColumns(Scan scan, String columns) {
		String[] cols = columns.split(" ");
		for (String col : cols) {
			addColumn(scan, Bytes.toBytes(col));
		}
	}
	
	private static void addColumn(Scan scan, byte[] familyAndQualifier) {
	    byte [][] fq = KeyValue.parseColumn(familyAndQualifier);
	    if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
	      scan.addColumn(fq[0], fq[1]);
	    } else {
	      scan.addFamily(fq[0]);
	    }
	  }
}
