package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstant;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                ""
        );
        while (true){
            canalConnector.connect();
            canalConnector.subscribe("gmall2020.*");

            Message message = canalConnector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();

            if (entries.size()<=0){
                System.out.println("当前抓取没有数据！！！");
                Thread.sleep(3000);
            }else {
                for (CanalEntry.Entry entry : entries) {
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())){

                        String tableName = entry.getHeader().getTableName();

                        ByteString storeValue = entry.getStoreValue();

                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        CanalEntry.EventType eventType = rowChange.getEventType();

                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        handler(tableName, eventType, rowDatasList);


                    }
                }
            }


        }

    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if ("order_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            for (CanalEntry.RowData rowData : rowDatasList) {
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(), column.getValue());
                }
                System.out.println(jsonObject);
                MyKafkaSender.send(GmallConstant.GMALL_ORDER_INFO, jsonObject.toString());
            }
        }else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstant.GMALL_ORDER_DETAIL);

        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            sendToKafka(rowDatasList, GmallConstant.GMALL_USER_INFO);
        }

    }
    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic){
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            try {
                Thread.sleep(new Random().nextInt(5)*1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(jsonObject);
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }


}
