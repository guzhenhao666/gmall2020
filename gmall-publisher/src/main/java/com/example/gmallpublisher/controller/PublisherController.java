package com.example.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.example.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {

        //1.创建集合用于存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //2.构建日活的Map
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", publisherService.getDauTotal(date));

        //3.构建新增设备的Map
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);

        //4.构建GmvMap
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", publisherService.getAmountTotal(date));



        //5.将Map放入集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);

        //6.返回结果
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getRealTimeHours(@RequestParam("id") String id,
                                   @RequestParam("date") String date) {

        //1.创建map用于存放结果数据
        HashMap<String, Map> result = new HashMap<>();

        //获取昨天日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        //声明用于存放今天和昨天数据的Map
        Map todayMap = null;
        Map yesterdayMap = null;

        if ("dau".equals(id)) {
            //2.获取今天的分时数据
            todayMap = publisherService.getDauTotalHourMap(date);
            //3.获取昨天的分时数据
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        } else if ("order_amount".equals(id)) {
            //2.获取今天的分时数据
            todayMap = publisherService.getOrderAmountHourMap(date);
            //3.获取昨天的分时数据
            yesterdayMap = publisherService.getOrderAmountHourMap(yesterday);
        }

        //4.将今天和昨天的数据放入结果集
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //5返回结果
        return JSONObject.toJSONString(result);
    }
    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") int startpage,
                                @RequestParam("size") int size,
                                @RequestParam("keyword") String keyword) throws IOException {

        return publisherService.getSaleDetail(date, startpage, size, keyword);
    }


}
