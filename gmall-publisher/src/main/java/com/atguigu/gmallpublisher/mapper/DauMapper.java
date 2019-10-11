package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    public long getTotal(String date);

    public List<Map> getDauHours(String id, String date);
}
