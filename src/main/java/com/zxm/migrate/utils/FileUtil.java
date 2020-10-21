package com.zxm.migrate.utils;

import cn.hutool.setting.dialect.Props;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.*;

/**
 * @author Jason
 */
public class FileUtil {

    /**
     * 读取迁移工具配置文件
     * @param migratePath
     * @param key
     * @return
     */
    public static String readMigrateProperties(String migratePath,String key) {

        Props props = new Props(migratePath);
        String value = props.getProperty(key);
        if(StringUtils.isNotEmpty(value)){
            return value;
        }else {
            throw new RuntimeException("key = '"+key+"' not found : migrate.properties");
        }
    }

    /**
     * 获取数据库连接信息
     *
     * @param path
     * @return
     */
    public static LinkedHashMap<Integer, HashMap<String, String>> getServerProperties(String path){

        List<String[]> list = POIUtil.readExcel(new File(path));

        LinkedHashMap<Integer, HashMap<String, String>> map = new LinkedHashMap<>();
        for (int i = 1; i < list.size(); i++) {
            HashMap<String, String> hashMap = new HashMap<>();
            String[] split = list.get(i);
            if (split != null) {

                if (StringUtils.isEmpty(split[4] )|| StringUtils.isBlank(split[4])) {
                    String sourceIp = split[0];
                    String sourcePort = split[1];
                    String sourceUsername = split[2];
                    String sourcePassword = split[3];
                    String targetIp = split[5];
                    String targetPort = split[6];
                    String targetUsername = split[7];
                    String targetPassword = split[8];

                    hashMap.put("sourceIp", sourceIp);
                    hashMap.put("sourcePort", sourcePort);
                    hashMap.put("sourceUsername", sourceUsername);
                    hashMap.put("sourcePassword", sourcePassword);
                    hashMap.put("targetIp", targetIp);
                    hashMap.put("targetPort", targetPort);
                    hashMap.put("targetUsername", targetUsername);
                    hashMap.put("targetPassword", targetPassword);
                } else {
                    String sourceIp = split[0];
                    String sourcePort = split[1];
                    String sourceUsername = split[2];
                    String sourcePassword = split[3];
                    String sourceDb = split[4];
                    String targetIp = split[5];
                    String targetPort = split[6];
                    String targetUsername = split[7];
                    String targetPassword = split[8];
                    String targetDb = split[9];

                    hashMap.put("sourceIp", sourceIp);
                    hashMap.put("sourcePort", sourcePort);
                    hashMap.put("sourceUsername", sourceUsername);
                    hashMap.put("sourcePassword", sourcePassword);
                    hashMap.put("sourceDb", sourceDb);
                    hashMap.put("targetIp", targetIp);
                    hashMap.put("targetPort", targetPort);
                    hashMap.put("targetUsername", targetUsername);
                    hashMap.put("targetPassword", targetPassword);
                    hashMap.put("targetDb", targetDb);
                }
            }
            map.put(i, hashMap);
        }
        return map;
    }

    /**
     * 读取表映射文件
     *
     * @param path 路径
     * @return 对应的表映射关系
     */
    public static LinkedHashMap<Integer, Object> getTableProperties(String path) {

        LinkedHashMap<Integer, Object> map;

        try {
            map = new LinkedHashMap<>();
            List<String[]> properties = POIUtil.readExcel(new File(path));

            for (int i = 1; i < properties.size(); i++) {

                HashMap<String, Object> hashMap = new HashMap<>(10);
                ArrayList<String> fromFieldList = new ArrayList<>();
                ArrayList<String> toFieldList = new ArrayList<>();
                String[] split = properties.get(i);

                if (StringUtils.isEmpty(split[0])) {
                    String fromTable = split[1];
                    Integer colNum = Integer.parseInt(split[2]);

                    String toTable = split[4 + colNum];
                    fromFieldList.addAll(Arrays.asList(split).subList(3, 3 + colNum));
                    toFieldList.addAll(Arrays.asList(split).subList(5 + colNum, split.length));

                    hashMap.put("fromTable", fromTable);
                    hashMap.put("fromFieldList", fromFieldList);
                    hashMap.put("colNum", colNum);
                    hashMap.put("toTable", toTable);
                    hashMap.put("toFieldList", toFieldList);
                } else if (!StringUtils.isEmpty(split[0])) {

                    String fromDb = split[0];
                    String fromTable = split[1];
                    Integer colNum = Integer.parseInt(split[2]);

                    String toDb = split[3 + colNum];
                    String toTable = split[4 + colNum];
                    fromFieldList.addAll(Arrays.asList(split).subList(3, 3 + colNum));
                    toFieldList.addAll(Arrays.asList(split).subList(5 + colNum, split.length));

                    hashMap.put("fromDb", fromDb);
                    hashMap.put("fromTable", fromTable);
                    hashMap.put("fromFieldList", fromFieldList);
                    hashMap.put("colNum", colNum);
                    hashMap.put("toDb", toDb);
                    hashMap.put("toTable", toTable);
                    hashMap.put("toFieldList", toFieldList);
                } else {
                    throw new RuntimeException("配置文件有误");
                }
                map.put(i, hashMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("table.xls parsing error");
        }
        return map;
    }
}
