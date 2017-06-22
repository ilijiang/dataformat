# 网签数据格式化导入

## 1. Download or Git clone project ##
```
    git@gitlab.esf.fangdd.net:esf_datawarehouse/data-format.git
```
## 2. Config Info ##
* HDFS Conf
```
    public static String hdfsFS = "hdfs://10.50.23.210:8020";
    public static String hdfsConf = "http://10.50.23.208:8080/hadoop-config/hdfs-site.xml";
    public static String coreConf = "http://10.50.23.208:8080/hadoop-config/core-site.xml";
    public static String implconf = "org.apache.hadoop.hdfs.DistributedFileSystem";
``` 
* Source File Dir
```
    private static String mappingFile = "/user/lijiang/network_data/mapping.csv";
    private static String zabeiFile = "/user/lijiang/network_data/zabei.csv";
    private static String zaibeiSectionFile = "/user/lijiang/network_data/zabei_section.csv";
```
```
    String sourceDir = "/user/lijiang/network_data/" + args[0];
    String backupDir = "/user/lijiang/network_data/backup/" + args[0] + "/";
```
* Target File Dir
```
    private static String targitDir1 = "/data/network_sign_data/";
    private static String targitDir2 = "/data/network_sign_data_weekly/";
```


## 3. Time Schedule ##
* step1 打包成可运行.Jar文件，上传到线上HDFS

* step2 workflow参数设置
    * parm1 数据类型："month"、"week"、"
    * parm2 文件编码："GBK"
    * parm3 日期："yyyyMM"、"yyyyMMdd"