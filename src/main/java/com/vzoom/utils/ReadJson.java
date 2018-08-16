package com.vzoom.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * 读取文件,将文件中的json字符,转换成需要插入的sql语句
 * 
 * @author root
 *
 */
public class ReadJson {

	public static void main(String[] args) throws IOException {
		readFile("F:\\wkspe\\logs\\数据补充.txt");
	}
	public static void readFile(String fileName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String str = null;
		while ((str = br.readLine()) != null) {
			System.out.println(str);
		}
		br.close();
	}
}
