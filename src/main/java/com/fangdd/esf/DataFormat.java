package com.fangdd.esf;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by lijiang on 5/27/17.
 */
public class DataFormat {

    public static final Logger logger = Logger.getLogger(DataFormat.class);

    private static String mappingFile = "/user/lijiang/network_data/mapping.csv";
    private static String zabeiFile = "/user/lijiang/network_data/zabei.csv";
    private static String zaibeiSectionFile = "/user/lijiang/network_data/zabei_section.csv";


    private static String targitDir1 = "/data/network_sign_data/";
    private static String targitDir2 = "/data/network_sign_data_weekly/";

    private static HashMap<String, String> mappingData = new HashMap<String, String>();
    private static HashMap<String, String> zabeiData = new HashMap<String, String>();
    private static List<String> zabeiSectionData = new ArrayList<String>();


    public static void main(String[] args) throws Exception{
        try {

            String sourceDir = "/user/lijiang/network_data/" + args[0];
            String backupDir = "/user/lijiang/network_data/backup/" + args[0] + "/";
            FileStatus[] fileStatuses = HdfsHelper.list(sourceDir);

            logger.info("文件读取成功：" + sourceDir);

            mappingData = mappingData();
            zabeiData = zabeiData();
            zabeiSectionData = zabeiSectionData();

            logger.info("mapping文件读取成功");

            for (FileStatus status : fileStatuses) {

                int count = 0;
                count ++;
                if (args[0].equals("month")) {
                    dataFormatMonthly(status.getPath().toString(), args[1], args[2],count);
                }
                if (args[0].equals("week")) {
                    dataFormatWeekly(status.getPath().toString(), args[1], args[2],count);
                }

                logger.info("格式化完成："+status.getPath().getName());

                HdfsHelper.move(status.getPath().toString(),backupDir);

                logger.info("源文件备份完成：" + backupDir);
            }


        } catch (Exception e) {

            logger.error(e.getMessage());
            throw e;
        }

    }

    public static void dataFormatMonthly(String sourceFilePath, String encoding, String month,Integer index) throws Exception {

        List<String> contents = HdfsHelper.reader(sourceFilePath, encoding);

        String targetFile = targitDir1 + month + "/fdd-sh-esf-" + month + "-" + index.toString() + ".txt";



        List<String> formatContents = new ArrayList<String>();

        for (String line : contents) {
            int count = 0;
            count++;
            String parts[] = line.split("\t");
            String districtName = parts[0].replace("|"," ");
            String sectionName = parts[1].replace("|"," ");
            String cellName = parts[3].replace("|"," ");
            String address = parts[4].replace("|"," ");
            String houseType = parts[6].replace("|"," ");
            float totalPrice = Float.valueOf(parts[7]);
            float prePrice = Float.valueOf(parts[8]);

            if (totalPrice <= 1000000 && address.contains("车") && houseType.contains("其它")) {
                continue;
            }
            if (prePrice <= 4000) {
                continue;
            }
            if (houseType.contains("办公楼") || houseType.contains("工厂") || houseType.contains("商铺")) {
                continue;
            }

            districtName = zaibeiFormat(districtName, cellName, sectionName);

            String key = districtName + sectionName + cellName;

            if (mappingData.containsKey(key)) {

                String mappedValue = mappingData.get(key);
                String[] mappedParts = mappedValue.split(",");
                String mappedCellName = mappedParts[0];
                String mappedDistrictName = mappedParts[1];
                formatContents.add(mappedDistrictName + "|" + sectionName + "|" + parts[2] + "|" + mappedCellName + "|" + parts[4] + "|" + parts[5] + "|" + parts[6] + "|" + parts[7] + "|" + parts[8] + "|" + parts[9] + "|" + parts[10] + "|" + parts[11]);
            } else {
                formatContents.add(districtName + "|" + sectionName + "|" + parts[2] + "|" + cellName + "|" + parts[4] + "|" + parts[5] + "|" + parts[6] + "|" + parts[7] + "|" + parts[8] + "|" + parts[9] + "|" + parts[10] + "|" + parts[11]);
            }

        }

        HdfsHelper.writer(targetFile, formatContents);
    }


    public static void dataFormatWeekly(String sourceFileName, String encoding, String dt, Integer index) throws Exception {


        List<String> contents = HdfsHelper.reader(sourceFileName, encoding);

        String targetFile = targitDir2 + dt + "/" + "/fdd-sh-esf-" + dt + "-" + index.toString() + ".txt";

        List<String> formatContents = new ArrayList<String>();

        for (String line : contents) {
            System.out.println(line);
            int count = 0;
            count++;
            String parts[] = line.split("\t");
            String districtName = parts[0].replace("|"," ");
            String sectionName = parts[1].replace("|"," ");
            String cellName = parts[3].replace("|"," ");
            String address = parts[4].replace("|"," ");
            float totalPrice = Float.valueOf(parts[6]);
            float prePrice = Float.valueOf(parts[7]);
            String houseType = parts[8].replace("|"," ");

            if (totalPrice <= 1000000 && address.contains("车") && houseType.contains("其它")) {
                continue;
            }
            if (prePrice <= 4000) {
                continue;
            }
            if (houseType.contains("办公楼") || houseType.contains("工厂") || houseType.contains("商铺")) {
                continue;
            }

            districtName = zaibeiFormat(districtName, cellName, sectionName);

            String key = districtName + sectionName + cellName;

            if (mappingData.containsKey(key)) {

                String mappedValue = mappingData.get(key);
                String[] mappedParts = mappedValue.split(",");
                String mappedCellName = mappedParts[0];
                String mappedDistrictName = mappedParts[1];
                formatContents.add(mappedDistrictName + "|" + sectionName + "|" + parts[2] + "|" + mappedCellName + "|" + parts[4] + "|" + parts[5] + "|" + parts[6] + "|" + parts[7] + "|" + parts[8] + "|" + parts[9] + "|" + parts[10]);
            } else {
                formatContents.add(districtName + "|" + sectionName + "|" + parts[2] + "|" + cellName + "|" + parts[4] + "|" + parts[5] + "|" + parts[6] + "|" + parts[7] + "|" + parts[8] + "|" + parts[9] + "|" + parts[10]);
            }

        }

        HdfsHelper.writer(targetFile, formatContents);
    }

    public static HashMap<String, String> mappingData() throws Exception {

        HashMap<String, String> mappingData = new HashMap<String, String>();
        List<String> contents = HdfsHelper.reader(new Path(mappingFile));

        for (String line : contents) {
            String columns[] = line.split(",");
            if (columns.length == 7) {
                String key = columns[0] + columns[1] + columns[2];
                String value = columns[5] + "," + columns[6];
                mappingData.put(key, value);
            }
        }

        return mappingData;
    }


    public static HashMap<String, String> zabeiData() throws Exception {

        HashMap<String, String> zabeiData = new HashMap<String, String>();
        List<String> contents = HdfsHelper.reader(new Path(zabeiFile));

        for (String line : contents) {
            String columns[] = line.split(",");
            if (columns.length == 2) {
                String key = columns[0];
                String value = columns[1];
                zabeiData.put(key, value);
            }
        }
        return zabeiData;
    }


    public static List<String> zabeiSectionData() throws Exception {

        List<String> zabeiSectionData = new ArrayList<String>();
        List<String> contents = HdfsHelper.reader(new Path(zaibeiSectionFile));

        for (String line : contents) {
            zabeiSectionData.add(line);
        }
        return zabeiSectionData;
    }

    public static String zaibeiFormat(String districtName, String cellName, String sectionName) throws Exception {

        if (districtName.equalsIgnoreCase("静安")) {
            if (zabeiData.containsKey(cellName)) {
                districtName = "闸北";
            } else if (zabeiSectionData.contains(sectionName)) {
                districtName = "闸北";
            }

        }
        return districtName;
    }

}
