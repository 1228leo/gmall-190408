package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.contants.GmallContants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import static com.atguigu.contants.GmallContants.GMALL_EVENT;
import static com.atguigu.contants.GmallContants.GMALL_STARTUP;

/**
 * Create : 2019/10/10 19:47
 * Create By Leo
 */
@RestController
@Slf4j
public class LogController {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping(value = "test")
    @ResponseBody
    public String getTest() {
        System.out.println("!!!!!!!!");
        return "success";
    }

    @PostMapping("log")
    public String doLog(@RequestParam(value = "logString") String str) {
        //1.添加时间戳
        JSONObject jsonObject = JSON.parseObject(str);
        jsonObject.put("ts", System.currentTimeMillis());
        String jsonStr = jsonObject.toString();
        //2.落盘
        log.info(jsonStr);
        //3.写入kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GMALL_STARTUP, jsonStr);
        } else {
            kafkaTemplate.send(GMALL_EVENT, jsonStr);
        }
        return "success";
    }
}
