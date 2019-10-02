package com.wangwren.config;

/**
 * 定义分布式配置，获得到的数据都放在这
 *
 * 主要还是看这个类，按照实际业务场景来
 */
public class MyConf {

    private String conf;

    public String getConf() {
        return conf;
    }

    public void setConf(String conf) {
        this.conf = conf;
    }
}
