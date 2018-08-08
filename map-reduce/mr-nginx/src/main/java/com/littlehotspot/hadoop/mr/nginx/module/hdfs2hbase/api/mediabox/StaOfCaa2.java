/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.box
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:38
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediabox;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * 手机日志
 */
public class StaOfCaa2 extends Configured implements Tool {

    //    private HBaseHelper hBaseHelper;
//
//
//    private String areasJson;
//    private String hotelsJson;
//    private String boxesJson;
//    private String tvsJson;
//
//
//    private static class MobileMapper extends TableMapper<Text, Text> {
//
//        private SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
//
//        @Override
//        protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
//            String row = Bytes.toString(result.getRow());
//            String mediaId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mda_id")));
//            if (StringUtils.isBlank(mediaId)) {
//                return;
//            }
//            String hotelId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_id")));
//            String hotelName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_name")));
//            String roomId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_id")));
//            String roomName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_name")));
//            String mac = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac")));
//            String mediaType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mda_type")));
//            String optionType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("option_type")));
//            String timestamps = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("timestamps")));
//
//            SourceBean sourceBean = new SourceBean();
//            sourceBean.setHotelId(hotelId);
//            sourceBean.setHotelName(hotelName);
//            sourceBean.setRoomId(roomId);
//            sourceBean.setRoomName(roomName);
//            sourceBean.setMac(mac);
//            sourceBean.setMediaId(mediaId);
//            sourceBean.setMediaType(mediaType);
//            sourceBean.setPlayDate(this.stampToDate(timestamps));
//            sourceBean.setTimestamps(timestamps);
//
//            String mapperKey = String.format("%s|%s|%s|%s|%s", sourceBean.getHotelId(), sourceBean.getRoomId(), sourceBean.getMac(), sourceBean.getMediaId(), sourceBean.getPlayDate());
//            context.write(new Text(mapperKey), new Text(sourceBean.rowLine1()));
//        }
//
//        private String stampToDate(String dateString) {
//            long lt = Long.valueOf(dateString);
//            Date date = new Date(lt);
//            return SIMPLE_DATE_FORMAT.format(date);
//        }
//    }
//
//    private static class MobileReduce extends Reducer<Text, Text, Text, Text> {
//
//        private HBaseHelper hBaseHelper;
//
//        private Map<String, Object> areaMap = new ConcurrentHashMap<>();
//        private Map<String, SavorHotel> hotelMap = new ConcurrentHashMap<>();
//        private Map<String, Object> boxMap = new ConcurrentHashMap<>();
//        private Map<String, Object> tvMap = new ConcurrentHashMap<>();
//
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            Configuration conf = context.getConfiguration();
//            this.hBaseHelper = new HBaseHelper(conf);
//
//            List<Result> hotelResultList = hBaseHelper.getAllRecord("hotel");
//            for (Result result : hotelResultList) {
//                byte[] rowKeyBytes = result.getRow();
//                String rowKey = Bytes.toString(rowKeyBytes);
//                long id=Long.valueOf();
//
//                SavorHotel savorHotel = new SavorHotel();
//                savorHotel.setId();
//                savorHotel.setArea_id();
//                savorHotel.setIskey();
//                savorHotel.setMaintainer();
//                savorHotel.setName();
//            }
//
//
//            List<SavorHotel> hotelList = JSONUtil.JSONArrayToList(hotels, SavorHotel.class);
//            for (SavorHotel o : hotelList) {
//                SavorHotel hotel = (SavorHotel) o;
//                this.hotelMap.put(String.valueOf(hotel.getId()), hotel);
//            }
//
//            List<Object> tvList = JSONUtil.JSONArrayToList(tvs, SavorTv.class);
//            for (Object o : tvList) {
//                SavorTv tv = (SavorTv) o;
//                this.tvMap.put(String.valueOf(tv.getId()), tv);
//            }
//
//            List<Object> boxList = JSONUtil.JSONArrayToList(boxes, SavorBox.class);
//            for (Object o : boxList) {
//                SavorBox box = (SavorBox) o;
//                this.boxMap.put(String.valueOf(box.getMac()), box);
//            }
//
//            List<Object> areaList = JSONUtil.JSONArrayToList(areas, SavorArea.class);
//            for (Object o : areaList) {
//                SavorArea area = (SavorArea) o;
//                this.areaMap.put(String.valueOf(area.getId()), area);
//            }
//
//        }
//
//        @Override
//        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
//            try {
//                Iterator<Text> textIterator = value.iterator();
//                SourceBean bean = new SourceBean();
//                Integer count = 0;
//                bean.setRowKey(key.toString());
//                while (textIterator.hasNext()) {
//                    Text item = textIterator.next();
//                    if (item == null) {
//                        continue;
//                    }
//                    String rowLineContent = item.toString();
//                    Matcher matcher = CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(rowLineContent);
//                    if (!matcher.find()) {
//                        return;
//                    }
//
//                    bean.setHotelId(matcher.group(1));
//                    bean.setHotelName(matcher.group(2));
//                    bean.setRoomId(matcher.group(3));
//                    bean.setRoomName(matcher.group(4));
//                    bean.setMac(matcher.group(5));
//                    bean.setPlayDate(StringUtils.trim(matcher.group(8)));
//
//                    if (StringUtils.isBlank(bean.getArea())) {
//                        Result hotelResult = hBaseHelper.getOneRecord("hotel", matcher.group(1));
//                        String areaId = new String(hotelResult.getValue(Bytes.toBytes("attr"), Bytes.toBytes("area_id")));
//                        SavorArea area = this.readMysqlArea(hotel.getArea_id().toString());
//                        bean.setArea(area.getRegion_name());
//                        bean.setAreaId(Long.valueOf(area.getId()).toString());
//                    }
//
//                    Result medias = hBaseHelper.getOneRecord("medias", matcher.group(6));
//
//                    if (medias.isEmpty()) {
//                        System.out.println(item.toString() + ": RESULT IS EMPTY");
//                        return;
//                    }
//                    bean.setMediaId(matcher.group(6));
//
//                    String duration = new String(medias.getValue(Bytes.toBytes("attr"), Bytes.toBytes("duration")));
//                    if (StringUtils.isBlank(bean.getPlayTime())) {
//                        bean.setPlayTime(duration);
//                    } else {
//                        Long playtime = Long.valueOf(bean.getPlayTime()) + Long.valueOf(duration);
//                        bean.setPlayTime(playtime.toString());
//                    }
//                    String mediaName = new String(medias.getValue(Bytes.toBytes("attr"), Bytes.toBytes("name")));
//                    bean.setMediaName(mediaName);
//
//
//                    count++;
//
//                }
//                bean.setPlayCount(count.toString());
//                context.write(new Text(bean.rowLine2()), new Text());
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//
//        /**
//         * 查询酒店信息
//         *
//         * @throws Exception
//         */
//        public SavorHotel readMysqlHotel(String hotelId) throws Exception {
//            if (this.hotelMap == null || this.hotelMap.get(hotelId) == null || this.hotelMap.size() <= 0) {
//                return null;
//            }
//            return (SavorHotel) this.hotelMap.get(hotelId);
//
//        }
//
//
//        public SavorArea readMysqlArea(String areaId) throws Exception {
//            if (this.areaMap == null || this.areaMap.get(areaId) == null || this.areaMap.size() <= 0) {
//                return null;
//            }
//            return (SavorArea) this.areaMap.get(areaId);
//
//        }
//
//
//        public SavorBox readMysqlBox(String mac) throws Exception {
//            if (this.boxMap == null || this.boxMap.get(mac) == null || this.boxMap.size() <= 0) {
//                return null;
//            }
//            return (SavorBox) this.boxMap.get(mac);
//
//        }
//
//
//        public Integer readMysqlTv(Long boxId) throws Exception {
//
//            Integer count = 0;
//            for (String s : tvMap.keySet()) {
//                SavorTv o = (SavorTv) tvMap.get(s);
//                if (o.getBox_id() == boxId) {
//                    count++;
//                }
//            }
//
//
//            return count;
//
//        }
//
//    }
//
//    @Override
    public int run(String[] args) throws Exception {
//        try {
//
//            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
//
//            // 获取参数
//            String hbaseSharePath = CommonVariables.getParameterValue(Argument.HBaseSharePath);
//            String hdfsCluster = CommonVariables.getParameterValue(Argument.HDFSCluster);
//            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);
//            String time = CommonVariables.getParameterValue(Argument.Time);
//            String before = CommonVariables.getParameterValue(Argument.Before);
//
//            this.hBaseHelper = new HBaseHelper(this.getConf());
//
//            // 查询mysql
//            findByMysql();
//            this.getConf().set("hotels", this.hotels);
//            this.getConf().set("tvs", this.tvs);
//            this.getConf().set("boxes", this.boxes);
//            this.getConf().set("areas", this.areas);
//
//            Job job = Job.getInstance(this.getConf(), StaOfCaa2.class.getSimpleName());
//            job.setJarByClass(StaOfCaa2.class);
//
//            // 避免报错：ClassNotFoundError hbaseConfiguration
//            Configuration jobConf = job.getConfiguration();
//            FileSystem hdfs = FileSystem.get(new URI(hdfsCluster), jobConf);
//            Path hBaseSharePath = new Path(hbaseSharePath);
//            FileStatus[] hBaseShareJars = hdfs.listStatus(hBaseSharePath);
//            for (FileStatus fileStatus : hBaseShareJars) {
//                if (!fileStatus.isFile()) {
//                    continue;
//                }
//                Path archive = fileStatus.getPath();
//                FileSystem fs = archive.getFileSystem(jobConf);
//                DistributedCache.addArchiveToClassPath(archive, jobConf, fs);
//            }//
//
//
//            Scan scan = new Scan();
//
//            //设置过滤器
//            List<Filter> filters = new ArrayList<Filter>();
//
//            RegexStringComparator comp = new RegexStringComparator("^(ads)|(pro)|(adv)$");
//            SingleColumnValueFilter typefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
//                    Bytes.toBytes("mda_type"), CompareFilter.CompareOp.EQUAL, comp);
//            SingleColumnValueFilter optionfilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
//                    Bytes.toBytes("option_type"), CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("start")));
////            if (!StringUtils.isBlank(time)){
////                RegexStringComparator comps = new RegexStringComparator("^"+"time");
////                SingleColumnValueFilter timefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
////                        Bytes.toBytes("date_time"), CompareFilter.CompareOp.EQUAL,comps);
////                filters.add(timefilter);
////            }
//
//            if (!StringUtils.isBlank(time) && !StringUtils.isBlank(before)) {
//                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
//                Calendar now = Calendar.getInstance();
//                now.setTime(format.parse(time));
//                now.set(Calendar.DATE, now.get(Calendar.DATE) - Integer.parseInt(before));
//                String day = format.format(now.getTime());
//                SingleColumnValueFilter timefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
//                        Bytes.toBytes("date_time"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(day + "00"));
//                filters.add(timefilter);
//            } else if (!StringUtils.isBlank(time) && StringUtils.isBlank(before)) {
//                RegexStringComparator comps = new RegexStringComparator("^" + "time");
//                SingleColumnValueFilter timefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
//                        Bytes.toBytes("date_time"), CompareFilter.CompareOp.EQUAL, comps);
//                filters.add(timefilter);
//            }
//
//            if (null == scan) {
//                System.out.println("error : scan = null");
//                return 1;
//
//            }
//
//            filters.add(typefilter);
//            filters.add(optionfilter);
//
//            FilterList filterList = new FilterList(filters);
//            scan.setFilter(filterList);
//            TableMapReduceUtil.initTableMapperJob("box_log", scan, MobileMapper.class, Text.class, Text.class, job, false);
//
//
//            /**作业输出*/
//            Path outputPath = new Path(hdfsOutputPath);
//            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), this.getConf());
//            if (fileSystem.exists(outputPath)) {
//                fileSystem.delete(outputPath, true);
//            }
//
//            FileOutputFormat.setOutputPath(job, outputPath);
//            job.setReducerClass(MobileReduce.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(Text.class);
//
//            boolean status = job.waitForCompletion(true);
//            if (!status) {
//                throw new Exception("MapReduce task execute failed.........");
//            }
        return 0;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return 1;
//        }
    }
//
//    public static boolean isYesterday(long time) {
//        boolean isYesterday = false;
//        Date date;
//        try {
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//            date = sdf.parse(sdf.format(new Date()));
//            if (time < date.getTime() && time > (date.getTime() - 24 * 60 * 60 * 1000)) {
//                isYesterday = true;
//            }
//        } catch (ParseException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        return isYesterday;
//    }
//
//    /**
//     * 查询mysql数据库
//     *
//     * @throws SQLException
//     * @throws IOException
//     * @throws JSONException
//     */
//    private void findByMysql() throws SQLException, IOException, JSONException {
//        IJDBCTools jdbcUtil = new JDBCTools(MysqlCommonVariables.driver, MysqlCommonVariables.dbUrl, MysqlCommonVariables.userName, MysqlCommonVariables.passwd);
//        try {
//            jdbcUtil.getConnection();
//            this.findAreas(jdbcUtil);
//            this.findHotels(jdbcUtil);
//            this.findBoxes(jdbcUtil);
//            this.findTvs(jdbcUtil);
//        } finally {
//            jdbcUtil.releaseConnection();
//        }
//    }
//
//
//    // ========================================== MYSQL 读取数据库 ========================================== \\
//    private void findAreas(IJDBCTools jdbcTools) throws IOException, JSONException, SQLException {
//        String sql = "SELECT id,region_name FROM savor_area_info";
//        List<SavorArea> result = jdbcTools.findResult(SavorArea.class, sql);
//        this.areasJson = JSONUtil.listToJsonArray(result).toString();
//    }
//
//    private void findHotels(IJDBCTools jdbcTools) throws IOException, JSONException, SQLException {
//        String sql = "SELECT id,name,area_id FROM savor_hotel";
//        List<SavorHotel> result = jdbcTools.findResult(SavorHotel.class, sql);
//        this.hotelsJson = JSONUtil.listToJsonArray(result).toString();
//    }
//
////    private void findRooms(JDBCTool jdbcUtil) throws IOException, JSONException, SQLException {
////        String sql = "SELECT id,name,mac FROM savor_box";
////        List<SavorBox> result = jdbcUtil.findResult(SavorBox.class, sql);
////        this.boxesJson = JSONUtil.listToJsonArray(result).toString();
////    }
//
//    private void findBoxes(IJDBCTools jdbcTools) throws IOException, JSONException, SQLException {
//        String sql = "SELECT id,name,mac FROM savor_box";
//        List<SavorBox> result = jdbcTools.findResult(SavorBox.class, sql);
//        this.boxesJson = JSONUtil.listToJsonArray(result).toString();
//    }
//
//    private void findTvs(IJDBCTools jdbcTools) throws IOException, JSONException, SQLException {
//        String sql = "SELECT box_id FROM savor_tv";
//        List<SavorTv> result = jdbcTools.findResult(SavorTv.class, sql);
//        this.tvsJson = JSONUtil.listToJsonArray(result).toString();
//    }
}
