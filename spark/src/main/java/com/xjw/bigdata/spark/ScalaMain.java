package com.xjw.bigdata.spark;

public class ScalaMain {

    public static void main(String[] args) {
        System.out.println("在java中引用scala");
        //调用 ScalaHello.scala 方法
        ScalaHello scalaHello = new ScalaHello();
        scalaHello.sayHello("scala");
        scalaHello.wordcount();
    }

}
