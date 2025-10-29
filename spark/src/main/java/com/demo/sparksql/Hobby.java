package com.demo.sparksql;

import lombok.Getter;
import lombok.Setter;

/**
 * @Author: 君墨笑
 * @Date: 2025/9/10 20:20
 */
@Getter
@Setter
public class Hobby {

    public Hobby(){}

    public Hobby(long id , String name, String description){
        this.name = name;
        this.description = description;
        this.id = id;
    }

    private long id;

    private String name;

    private String description;
}