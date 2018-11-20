package com.nextjoy.retention.enums;

/**
 * Created with IDEA by ChouFy on 2018/11/2.
 *
 * @author Zhoufy
 */
public enum LifeTimeValueDay {

    /**
     * 根据编号获取列名
     */
    day1(1, "day1"),
    day2(2, "day2"),
    day3(3, "day3"),
    day5(5, "day5"),
    day7(7, "day7"),
    day15(15, "day15"),
    day30(30, "day30"),
    day60(60, "day60"),
    day90(90, "day90");


    private int day;
    private String col;


    LifeTimeValueDay(int day, String col) {
        this.day = day;
        this.col = col;
    }

    public static String getCol(int day) {
        for (LifeTimeValueDay c : LifeTimeValueDay.values()) {
            if (c.getDay() == day) {
                return c.col;
            }
        }
        return null;
    }

    public int getDay() {
        return day;
    }

}
