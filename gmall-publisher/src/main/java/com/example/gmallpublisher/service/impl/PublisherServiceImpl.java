package com.example.gmallpublisher.service.impl;

import com.alibaba.fastjson.JSON;
import com.example.gmallpublisher.bean.Option;
import com.example.gmallpublisher.bean.Stat;
import com.example.gmallpublisher.mapper.DauMapper;
import com.example.gmallpublisher.mapper.GmvMapper;
import com.example.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
//import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
//import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Service
public class PublisherServiceImpl  implements PublisherService {


    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private GmvMapper gmvMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {

        //1.查询Phoenix获取数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map用于存放数据
        HashMap<String, Long> result = new HashMap<>();

        //3.遍历list
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        //4.返回数据
        return result;

    }

    @Override
    public Double getAmountTotal(String date) {
        return gmvMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHourMap(String date) {
        List<Map> list = gmvMapper.selectOrderAmountHourMap(date);

        HashMap<String, Double> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        return result;
    }

    @Override
    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException {
        //1.使用代码的方式构建DSL语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //1.1 添加全值匹配条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("dt", date);
        boolQueryBuilder.filter(termQueryBuilder);

        //1.2 分词匹配条件
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND);
        boolQueryBuilder.must(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        //1.3 添加年龄聚合组
        TermsBuilder countByAge = AggregationBuilders.terms("countByAge")
                .field("user_age")
                .size(1000);
        searchSourceBuilder.aggregation(countByAge);

        //1.4 添加性别聚合组
        TermsBuilder countByGender = AggregationBuilders.terms("countByGender")
                .field("user_gender")
                .size(3);
        searchSourceBuilder.aggregation(countByGender);

        //1.5 分页
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        //2. 执行查询
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("gmall2020_sale_detail-query")
                .addType("_doc")
                .build();
        SearchResult searchResult = jestClient.execute(search);

        //3. 解析查询结果
        //3.1 获取查询总数
        Long total = searchResult.getTotal();

        //3.2 获取查询明细
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        ArrayList<Map> detail = new ArrayList<>();
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detail.add(hit.source);
        }

        MetricAggregation aggregations = searchResult.getAggregations();

        //3.3 获取年龄聚合组数据
        TermsAggregation countByAgeAgg = aggregations.getTermsAggregation("countByAge");
        Long lower20Count = 0L;
        Long upper20Lower30Count = 0L;
        for (TermsAggregation.Entry entry : countByAgeAgg.getBuckets()) {
            if (20 > Integer.parseInt(entry.getKey())) {
                lower20Count += entry.getCount();
            } else if (30 > Integer.parseInt(entry.getKey())) {
                upper20Lower30Count += entry.getCount();
            }
        }

        double lower20Ratio = Math.round(lower20Count * 1000L / total) / 10D;
        double upper20Lower30Ratio = Math.round(upper20Lower30Count * 1000L / total) / 10D;
        double upper30Ratio = Math.round((100D - lower20Ratio - upper20Lower30Ratio) * 10D) / 10D;

        Option lower20Opt = new Option("20岁以下", lower20Ratio);
        Option upper20Lower30Opt = new Option("20岁到30岁", upper20Lower30Ratio);
        Option upper30Opt = new Option("30岁及30岁以上", upper30Ratio);

        ArrayList<Option> ageList = new ArrayList<>();
        ageList.add(lower20Opt);
        ageList.add(upper20Lower30Opt);
        ageList.add(upper30Opt);

        Stat ageStat = new Stat("用户年龄占比", ageList);

        //3.4 获取性别聚合组数据
        TermsAggregation countByGenderAgg = aggregations.getTermsAggregation("countByGender");
        Long maleCount = 0L;
        for (TermsAggregation.Entry entry : countByGenderAgg.getBuckets()) {
            if ("M".equals(entry.getKey())) {
                maleCount = entry.getCount();
            }
        }

        double maleRatio = Math.round(maleCount * 1000L / total) / 10D;
        double femaleRatio = 100D - maleRatio;

        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);

        ArrayList<Option> genderList = new ArrayList<>();
        genderList.add(maleOpt);
        genderList.add(femaleOpt);

        Stat genderStat = new Stat("用户性别占比", genderList);

        //创建集合用于存放2张饼图的数据
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //4.创建Map用于存放最终结果数据
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);

        //5.将集合转换为字符串返回
        return JSON.toJSONString(result);
    }
}
