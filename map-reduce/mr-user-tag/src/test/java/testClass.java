import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.junit.Test;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-09 上午 10:51.
 */
public class testClass {
    @Test
    public void test(){
//        long[] arr = new long[]{1,1,2,2,2,2,2,2,3,3,4,5,7,8};
//        System.out.println(isMark(arr));

        long time = 1497976462000l;
        SimpleDateFormat format = new SimpleDateFormat("YYYYMMdd");
        System.out.println(format.format(time));
        Date date1 = new Date(time);

        long time1 = 1498076462000l;
        System.out.println(format.format(time1));
        Date date2 = new Date(time1);

        System.out.println(daysOfTwo(date1,date2));


    }

    private int daysOfTwo(Date fDate, Date oDate) {

        Calendar aCalendar = Calendar.getInstance();

        aCalendar.setTime(fDate);

        int day1 = aCalendar.get(Calendar.DAY_OF_YEAR);

        aCalendar.setTime(oDate);

        int day2 = aCalendar.get(Calendar.DAY_OF_YEAR);

        return day2 - day1;

    }

    private boolean isMark(long[] arr){
        int count=1;
        boolean mark = false;
        for (int i = 0; i < arr.length-1; i++) {
            if(arr[i+1]-arr[i] == 1){
                count++;
            }else if(arr[i+1] == arr[i]){
                continue;
            }else{
                count = 0;
            }

            if(count == 5){
                // 标记
                mark = true;
                break;
            }
        }
        return mark;
    }
}
