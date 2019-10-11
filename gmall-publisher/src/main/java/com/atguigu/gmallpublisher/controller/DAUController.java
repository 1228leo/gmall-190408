package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.DauService;
import org.apache.commons.lang.time.DateUtils;
import org.mortbay.util.ajax.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Create : 2019/10/11 18:53
 * Create By Leo
 */
@RestController
public class DAUController {
    // http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
    @Autowired
    private DauService dauService;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {
        long total = dauService.getTotal(date);
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", total);
        list.add(dauMap);
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);
        list.add(newMidMap);
        return JSON.toString(list);
    }

    @GetMapping("realtime-hours")
    public String realtimeHourDate(@RequestParam("id") String id, @RequestParam("date") String date) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        // 当天的数据
        Map<String,Long> dauHoursToday  = dauService.getDauHours(id,date);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("today",dauHoursToday);
        // 昨日数据
        Date yesterday = DateUtils.addDays(sdf.parse(date), -1);
        Map<String,Long> dauHoursYesterday  = dauService.getDauHours(id,sdf.format(yesterday));
        jsonObject.put("yesterday",dauHoursYesterday);
        return jsonObject.toJSONString();
    }
}
