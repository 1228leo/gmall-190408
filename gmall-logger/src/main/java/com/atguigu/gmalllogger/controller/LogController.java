package com.atguigu.gmalllogger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Create : 2019/10/10 19:47
 * Create By Leo
 */
@Controller
public class LogController {
    @RequestMapping(value = "test")
    @ResponseBody
    public String getTest(){
        System.out.println("!!!!!!!!");
        return "success";
    }

    @PostMapping("log")
    public String doLog(@RequestParam String log){
        System.out.println("-----------");
        return log;
    }
}
