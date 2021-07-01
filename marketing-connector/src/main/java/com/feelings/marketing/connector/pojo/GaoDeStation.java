package com.feelings.marketing.connector.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/18 6:39 下午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class GaoDeStation {
    private Integer id;
    private String gd_id;
    private String name;
    private String type;
    private String type_code;
    private String province_name;
    private String city_name;
    private String adname;
    private String address;
    private Double lng;
    private Double lat;
    private String tel;
    private String create_time;
    private String update_time;
    private String location;
}
