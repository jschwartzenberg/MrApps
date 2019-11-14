package org.example.hadoop;

import static java.util.concurrent.Executors.newFixedThreadPool;

import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseWrkinsrt {

        public static void main(String[] args) throws IOException{

                // TODO Auto-generated method stub
                //Create a hbaseAdmin
              Configuration config = HBaseConfiguration.create();

           // Instantiating HTable class
              String string = "employeeH";
              Table hTable =  ConnectionFactory.createConnection(config).getTableBuilder(TableName.valueOf("employeeH"), newFixedThreadPool(1)).build();

              // Instantiating Put class
              // accepts a row name.
              Put p = new Put(Bytes.toBytes("row1"));
           // adding values using add() method
              // accepts column family name, qualifier/row name ,value
              p.add(createCell("personal", "name", "raju"));

              p.add(createCell("personal", "city", "hyderabad"));
              p.add(createCell("professional", "designation", "manager"));

              p.add(createCell("professional", "salary", "50000"));

              // Saving the put Instance to the HTable.
              hTable.put(p);
              System.out.println("data inserted");

              // closing HTable
              hTable.close();
        }

		private static Cell createCell(String family, String qualifier, String value) {
			return CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setFamily(family.getBytes()).setQualifier(qualifier.getBytes()).setValue(value.getBytes()).build();
		}

}
