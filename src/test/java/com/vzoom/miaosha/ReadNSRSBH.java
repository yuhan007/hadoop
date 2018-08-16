package com.vzoom.miaosha;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jettison.json.JSONArray;

import com.sun.tools.classfile.Annotation.element_value;

public class ReadNSRSBH {

	public static void main(String[] args) {
		String str = txt2String(new File("F:/wkspe/logs/catalina.2018-07-2711111.out"));
//		String rgex = "计费记录接口接收(.*?)所属年月不能为空"; 
//		String rgex = "报文(.*?)贷后产品,报"; 
//		write2TXT(getSubUtil(str,rgex));
		write2TXT(str);
//		String str = "abc3443abcfgjhgabcgfjabc";  
//        String rgex = "abc(.*?)abc";  
//        System.out.println(getSubUtil(str,rgex));  
//        System.out.println(getSubUtilSimple(str, rgex));
	}

	public static String txt2String(File file) {
		StringBuilder result = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));// 构造一个BufferedReader类来读取文件
			String s = null;
			while ((s = br.readLine()) != null ) {// 使用readLine方法，一次读一行
				if (s.indexOf("贷后产品")!=-1) {
					result.append(s);
					result.append("\n\n");
				}else{
					result.append(s);
				}
				
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString();
	}

	/**
	 * 将字符串写到txt中
	 */
	public static void write2TXT(String str) {
		FileWriter fw = null;
		File f = new File("F:/wkspe/logs/aaa1.txt");
		try {
			if (!f.exists()) {
				f.createNewFile();
			}
			fw = new FileWriter(f);
			BufferedWriter out = new BufferedWriter(fw);
			out.write(str, 0, str.length() - 1);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end");
	}

	public static String getSubUtil(String soap, String rgex) {
//		List<String> list = new ArrayList<String>();
		StringBuffer sb=new StringBuffer();
		Pattern pattern = Pattern.compile(rgex);// 匹配的模式
		Matcher m = pattern.matcher(soap);
		while (m.find()) {
			int i = 1;
			sb.append(m.group(i)+"\n");
			i++;
		}
		return sb.toString();
	}

	/**
	 * 返回单个字符串，若匹配到多个的话就返回第一个，方法与getSubUtil一样
	 * 
	 * @param soap
	 * @param rgex
	 * @return
	 */
	public static String getSubUtilSimple(String soap, String rgex) {
		Pattern pattern = Pattern.compile(rgex);// 匹配的模式
		Matcher m = pattern.matcher(soap);
		while (m.find()) {
			return m.group(1);
		}
		return "";
	}
}
