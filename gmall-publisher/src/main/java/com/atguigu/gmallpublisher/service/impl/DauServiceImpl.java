package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create : 2019/10/11 19:13
 * Create By Leo
 */
@Service
public class DauServiceImpl implements DauService {
    @Autowired
    private DauMapper dauMapper;
    @Override
    public long getTotal(String date) {
        return dauMapper.getTotal(date);
    }

    @Override
    public Map<String, Long> getDauHours(String id, String date) {
        HashMap dauHourMap=new HashMap();
        List<Map> dauHourList = dauMapper.getDauHours(id,date);
        for (Map map : dauHourList) {
            dauHourMap.put(map.get("LH"),map.get("CT"));
        }
        return dauHourMap;
    }
}
