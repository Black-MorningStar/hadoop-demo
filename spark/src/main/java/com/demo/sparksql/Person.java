package com.demo.sparksql;

/**
 * // 定义一个 JavaBean（必须有无参构造 & getter/setter）
 *
 * @Author: 君墨笑
 * @Date: 2025/9/10 19:21
 */
public class Person {

    private long id;
    private String name;
    private int age;

    public Person() {}
    public Person(long id, String name, int age) {
        this.id = id; this.name = name; this.age = age;
    }

    public long getId() { return id; }
    public void setId(long id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getAge() { return age; }
    public void setAge(int age) { this.age = age; }
}