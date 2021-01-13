//package com.atguigu.utils
//
//import java.io.InputStreamReader
//import java.util.Properties
//
//object proT1 {
//  def load(name: String) = {
//    val T1: Properties = new Properties
//    T1.load(new InputStreamReader(
//      Thread.currentThread()
//        .getContextClassLoader
//        .getResourceAsStream(name), "UTF-8"))
//    T1
//  }
//
//}
