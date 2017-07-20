package com.littlehotspot.util.excel;

import lombok.Data;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;

/**
 * excel实体类
 * Copyright : Copyright (c) 2015- 2015 All rights reserved. <br/>
 * @author liufeifei
 */
@Data
public class ExcelBean {
	/** 起始列编号,一般在自定义或合并单元格时需要设置 */
	private int startColNum;
	/** 起始行编号,一般在自定义或合并单元格时需要设置 */
	private int startRowNum;
	/** 结束列编号,一般在自定义或合并单元格时需要设置 */
	private int endColNum;
	/** 结束行编号,一般在自定义或合并单元格时需要设置 */
	private int endRowNum;
	/** 是否合并单元格 */
	private boolean isMerge;
	/** 设置水平对齐方式(注:默认情况下数字型右对齐,此属性对数字型数据无效) */
	private short alignment = HSSFCellStyle.ALIGN_LEFT;
	/** 设置字体 默认为宋体 */
	private String fontName = "宋体";
	/** 设置字体大小,默认大小为9 */
	private int fontSize = 9;
	/** 设置字体样式 是否为粗体 */
	private boolean isBord = false;
	/** 设置字体样式 是否加下划线 */
	private boolean isUnderline = false;
	/** 设置字体样式 是否为斜体 */
	private boolean isItalic = false;
	/** 设置字体颜色,默认为黑色 */
	private short fontColor = HSSFFont.COLOR_NORMAL;
	/** 设置边框 */
	private boolean isBorder = false;
	/** 是否为自定义单元格,此项设置为true时,待写入的数据不能是集合类型只能是基础类型,如:String */
	private boolean isUserDefined = false;
	/** 待写入的数据 ,多行数据写入时,此属性为封装的List<List>集合类型 */
	private Object data;

	public ExcelBean() {

	}
    /**
     * 设置合并的单元格
     * @param startColNum
     *                 : 开始列
     * @param startRowNum
     *                 : 开始行
     * @param endColNum
     *                 : 结束列
     * @param endRowNum
     *                 : 结束行
     */
	public ExcelBean(int startColNum,int startRowNum,int endColNum,int endRowNum) {
         this.isMerge = true ;
         this.startColNum = startColNum;
         this.startRowNum = startRowNum;
         this.endColNum = endColNum;
         this.endRowNum = endRowNum;
	}
}
