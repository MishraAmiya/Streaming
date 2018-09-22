import org.joda.time.DateTime;

/**
 * Created by AMIYA on 9/22/2018.
 */
public class MonthDifference {
    private int[] monthDay = {31, -1, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    private DateTime fromDate = null;
    private DateTime toDate = null;
    int year;
    int month;
    int day;

    public static void main(String[] args) {

        DateTime fromDate = DateTime.parse(args[0]);
        DateTime toDate = DateTime.parse(args[1]);

        new MonthDifference(fromDate, toDate);
    }

    MonthDifference(DateTime d1, DateTime d2) {
        if (d1.isAfter(d2.toInstant())) {
            this.fromDate = d2;
            this.toDate = d1;
        } else {
            this.fromDate = d1;
            this.toDate = d2;
        }

        System.out.println("From date is: " + this.fromDate);
        System.out.println("To date is: " + this.toDate);
        int increment = 0;
        if (this.fromDate.getDayOfMonth() > this.toDate.getDayOfMonth()) {
            increment = this.monthDay[this.fromDate.getMonthOfYear() - 1];
        }
        if (increment == -1)
        {
            if (this.fromDate.year().isLeap())
            {
                increment = 29;
            }
            else
            {
                increment = 28;
            }
        }
        if (increment != 0)
        {
            day = (this.toDate.getDayOfMonth() + increment) - this.fromDate.getDayOfMonth();
            increment = 1;
        }
        else
        {
            day = this.toDate.getDayOfMonth() - this.fromDate.getDayOfMonth();
        }
        if ((this.fromDate.getMonthOfYear() + increment) > this.toDate.getMonthOfYear())
        {
            this.month = (this.toDate.getMonthOfYear() + 12) - (this.fromDate.getMonthOfYear() + increment);
            increment = 1;
        }
        else
        {
            this.month = (this.toDate.getMonthOfYear() ) - (this.fromDate.getMonthOfYear()  + increment);
            increment = 0;
        }
        this.year = this.toDate.year().get() - (this.fromDate.year().get() + increment);
        System.out.println(this.year + "Year(s), " + this.month + " month(s), " + this.day + " day(s)");
    }
}