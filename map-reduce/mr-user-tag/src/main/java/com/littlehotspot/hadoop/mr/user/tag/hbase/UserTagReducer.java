package com.littlehotspot.hadoop.mr.user.tag.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-09 上午 11:03.
 */
public class UserTagReducer extends Reducer<Text, Text, Text, Text> {

    private Configuration conf;
    private List<String> savorMobileId = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String mark = "3";// 普通用户
        if (this.savorMobileId.contains(key.toString())) {//运维人员
            mark = "2";
        } else {// 酒楼人员
            List<Date> dates = new ArrayList<>();
            Iterator<Text> textIterator = values.iterator();
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }

                String timestamps = item.toString();
                if (timestamps.length() < 13) {
                    for (int i = 0; i < 13 - timestamps.length(); i++) {
                        timestamps += "0";
                    }
                }
                Date date = new Date(Long.valueOf(timestamps));
                dates.add(date);
            }
            if (isMark(dates)) {
                mark = "1";
            }
        }

        context.write(new Text(key.toString() + "|" + mark), new Text());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.conf = context.getConfiguration();
        this.savorMobileId.add("0911C065-D984-46EC-BEFE-53DC424D686D");
        this.savorMobileId.add("1458F88D-4D62-4E25-BA67-05BD55D03BC0");
        this.savorMobileId.add("860076031731317");
        this.savorMobileId.add("7151C17E-AA27-4CAB-A582-0D0316B97C9E");
        this.savorMobileId.add("863125031327738");
        this.savorMobileId.add("863274036064324");
        this.savorMobileId.add("563DB0E2-28D0-4C2B-B38A-21B5B8B5DDB1");
        this.savorMobileId.add("6EEB4D42-19CD-486B-841E-EA4D9C88D7C8");
        this.savorMobileId.add("D51057FB-1BC9-4FC7-A2A0-675CAD50B2A1");
        this.savorMobileId.add("866693029843955");
        this.savorMobileId.add("353952075610662");
        this.savorMobileId.add("5074C764-9105-4849-A489-EDE9F99A30F1");
        this.savorMobileId.add("141D38D-6B2C-43CD-A1B7-A80BD14747EF");
        this.savorMobileId.add("8F59450C-220F-4B92-B028-A0484C86F22A");
        this.savorMobileId.add("BA4979F1-34A8-45DE-AC2A-A94076AF1D70");
        this.savorMobileId.add("352709083931097");
        this.savorMobileId.add("225E39E8-21B0-4F59-9FE4-F93BC549FD3F");
        this.savorMobileId.add("2B930E0E-090D-4E9D-82CE-8E146C7ED6FE");
        this.savorMobileId.add("BA589B6A-9D46-498E-8D9E-A5957DD8F935");
        this.savorMobileId.add("1B59F44-403E-488C-B447-64DFE385C3AC");
        this.savorMobileId.add("865902032105224");
        this.savorMobileId.add("C91FA192-31DD-462E-AE82-3EAB16686080");
        this.savorMobileId.add("861545038107738");
        this.savorMobileId.add("1F947DD2-1D96-4D58-B3DD-650DAF098017");
        this.savorMobileId.add("8613650310081917");
        this.savorMobileId.add("864446027301522");
        this.savorMobileId.add("1BD79BFD-FECB-4BAE-A63A-56E88CDD2EA2");
        this.savorMobileId.add("353819080640937");
        this.savorMobileId.add("A093A141-02AE-41F6-811B-33F4B57ED072");
        this.savorMobileId.add("351956082010370");
        this.savorMobileId.add("355834080071809");
        this.savorMobileId.add("99000712872422");
        this.savorMobileId.add("352564072148983");
        this.savorMobileId.add("354983075136563");
        this.savorMobileId.add("5096028A-AED5-478D-9D2D-10D7FE3A4F57");
        this.savorMobileId.add("438C2BB4-1BF9-47BB-8AAB-28AACFC258B2");
        this.savorMobileId.add("861365031081917");
        this.savorMobileId.add("5DF33AEE-6F8E-4EDE-96FA-FDD6A1B9D0C1");
        this.savorMobileId.add("5025B967-DE89-47E7-AAC0-5772D22E43CB");
        this.savorMobileId.add("C685CEAE-0A63-4A0F-89E1-07533D55B9C9");
        this.savorMobileId.add("861353037565679");
        this.savorMobileId.add("5DF33AEE-6F8E-4EDE-96FA-FDD6A1B9D0C1");
        this.savorMobileId.add("34BD0B91-8893-459E-95D7-12116821158C");
        this.savorMobileId.add("0EC3D99A-0851-4ED2-A664-BF0FDDA11046");
        this.savorMobileId.add("862031030667283");
        this.savorMobileId.add("356832070065210");
        this.savorMobileId.add("E5D708A4-C38E-48AA-8579-F6BD51BCCC22");
        this.savorMobileId.add("869372029927129");
        this.savorMobileId.add("2D00A669-4E2D-4B40-8AEF-1B4F034BC366");
        this.savorMobileId.add("867831025266610");
        this.savorMobileId.add("355905072113252");
        this.savorMobileId.add("357536060681709");
        this.savorMobileId.add("352628062750593");
        this.savorMobileId.add("86106903086897");
        this.savorMobileId.add("438C2BB4-1BF9-47BB-8AAB-28AACFC258B2");
        this.savorMobileId.add("5B12C40C-EF11-4076-ABA0-1A91B62A13F2");
        this.savorMobileId.add("75213B3C-7016-4DE0-B9AA-4F5F458336C1");
        this.savorMobileId.add("738D9081-A037-47D7-BDCB-18DB3774F25E");
        this.savorMobileId.add("DD163B65-F341-48AF-89F4-37FD2D9C05DB");
        this.savorMobileId.add("863410031927012");
    }

    private boolean isMark(List<Date> arr) {
        int count = 1;
        boolean mark = false;
        for (int i = 0; i < arr.size() - 1; i++) {
            if (daysOfTwo(arr.get(i), arr.get(i + 1)) == 1) {
                count++;
            } else if (arr.get(i + 1) == arr.get(i)) {
                continue;
            } else {
                count = 0;
            }

            if (count == 5) {
                // 标记
                mark = true;
                break;
            }
        }
        return mark;
    }

    private int daysOfTwo(Date fDate, Date oDate) {

        Calendar aCalendar = Calendar.getInstance();

        aCalendar.setTime(fDate);

        int day1 = aCalendar.get(Calendar.DAY_OF_YEAR);

        aCalendar.setTime(oDate);

        int day2 = aCalendar.get(Calendar.DAY_OF_YEAR);

        return day2 - day1;

    }
}
