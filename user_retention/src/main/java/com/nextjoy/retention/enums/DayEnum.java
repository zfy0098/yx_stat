package com.nextjoy.retention.enums;

/**
 * @author hadoop
 */

public enum DayEnum {


    /**
     * 根据编号获取列名
     */
    day2(2, "day2"),
    day3(3, "day3"),
    day4(4, "day4"),
    day5(5, "day5"),
    day6(6, "day6"),
    day7(7, "day7"),
    day8(8, "day8"),
    day9(9, "day9"),
    day10(10, "day10"),
    day11(11, "day11"),
    day12(12, "day12"),
    day13(13, "day13"),
    day14(14, "day14"),
    day15(15, "day15"),
    day30(30, "day30"),
    day60(60, "day60"),
    day90(90, "day90");


    private int day;
    private String col;


    DayEnum(int day, String col) {
        this.day = day;
        this.col = col;
    }

    public static String getCol(int day) {
        for (DayEnum c : DayEnum.values()) {
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
