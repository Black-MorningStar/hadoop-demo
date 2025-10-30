package com.demo.structstreaming;

import lombok.Getter;
import lombok.Setter;

/**
 * @Author: 君墨笑
 * @Date: 2025/10/29 17:16
 */
@Getter
@Setter
public class WordCountEntity {

    public WordCountEntity(String word, int count, String desc) {
        this.count = count;
        this.word = word;
        this.desc = desc;
    }

    private String word;

    private String desc;

    private int count;
}