package com.example.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface GmvMapper {
    public Double selectOrderAmountTotal(String date);

    public List<Map> selectOrderAmountHourMap(String date);
}
