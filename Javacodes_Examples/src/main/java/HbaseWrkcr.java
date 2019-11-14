package org.example.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
public class HbaseWrkcr {

        public static void main(String[] args) throws IOException{
				// TODO Auto-generated method stub

              // Instantiating table descriptor class
        	TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("employeeH"));

              // Adding column families to table descriptor
              tableDescriptorBuilder.addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("personal".getBytes()).build());
              tableDescriptorBuilder.addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("professional".getBytes()).build());

               {
              //Create a hbaseAdmin
              Configuration config = HBaseConfiguration.create();
              Admin admin = ConnectionFactory.createConnection(config).getAdmin();


              // Execute the table through admin
              admin.createTable(tableDescriptorBuilder.build());
              System.out.println(" Table created ");
              }}}
