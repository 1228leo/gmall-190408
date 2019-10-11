package com.atguigu.gmallpublisher.service;

import java.util.Map;

/**
 * Create : 2019/10/11 19:13
 * Create By Leo
 */
public interface DauService {
    public long getTotal(String date);

    public Map<String, Long> getDauHours(String id, String date);
}
