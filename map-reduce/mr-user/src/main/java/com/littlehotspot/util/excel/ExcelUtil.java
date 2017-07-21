package com.littlehotspot.util.excel;

import org.apache.commons.lang.StringUtils;
import org.apache.http.impl.cookie.DateUtils;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.List;

/**
 * excel开发工具
 * @author liufeifei
 */
public class ExcelUtil{
	/** 格式化数字   **/
	public static DecimalFormat def = new DecimalFormat("0.00");
    /**
     * 根据模板，导出excel
     * @author liufeifei <br/>
     * @param targetPath
     * @param tempFilePath
     * @param excelBeans
     */
	public static void writeToExcelTemplet(String targetPath,
			String tempFilePath,ExcelBean... excelBeans) {
		if (StringUtils.isBlank(tempFilePath) || excelBeans == null) {
			return;
		}
		OutputStream fos=null;
		FileInputStream fis = null;
		XSSFWorkbook swb;
		Sheet sheet;
		try {
			File targetFile=new File(targetPath);
			if(!targetFile.exists()){
				targetFile.createNewFile();
			}
			fos=new FileOutputStream(targetPath);
			fis = new FileInputStream(tempFilePath);
			swb=new XSSFWorkbook(fis);
			fis.close();
			sheet = swb.getSheetAt(0);
			for (ExcelBean eb : excelBeans) {
				CellStyle style = swb.createCellStyle();
				Font font = swb.createFont();
				font.setFontName(eb.getFontName());
				font.setFontHeightInPoints((short) eb.getFontSize());
				font.setBoldweight(eb.isBord() ? HSSFFont.BOLDWEIGHT_BOLD
						: HSSFFont.BOLDWEIGHT_NORMAL);
				font.setColor(eb.getFontColor());
				font.setItalic(eb.isItalic() ? true : false);
				font.setUnderline(eb.isUnderline() ? HSSFFont.U_SINGLE
						: HSSFFont.U_NONE);
				style.setFont(font);
				style.setAlignment(eb.getAlignment());
				if (eb.isBorder()) {
					style.setBorderLeft(HSSFCellStyle.BORDER_THIN);
					style.setBorderRight(HSSFCellStyle.BORDER_THIN);
					style.setBorderTop(HSSFCellStyle.BORDER_THIN);
					style.setBorderBottom(HSSFCellStyle.BORDER_THIN);
				}
				if (eb.getData() instanceof List) {
					List rowList = (List) eb.getData();
					int startRowNum = eb.getStartRowNum();
					if (rowList != null && rowList.size() > 0) {
						for (int i = 0; i < rowList.size(); i++) {
							List cellList = (List) rowList.get(i);
							for (int j = 0; j < cellList.size(); j++) {
								Object content = cellList.get(j);
								buildCellDataType(swb, sheet, j, startRowNum,
										content, style, eb.isBorder());
							}
							startRowNum++;
						}
					}
				} else {
					buildCellDataType(swb, sheet, eb.getStartColNum(),
							eb.getStartRowNum(), eb.getData(), style,
							eb.isBorder());
					if (eb.isMerge()) {
						sheet.addMergedRegion(new CellRangeAddress(eb
								.getStartRowNum(), eb.getEndRowNum(), eb
								.getStartColNum(), eb.getEndColNum()));
					}
				}
			}
			swb.write(fos);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (fis != null) {
					fis.close();
				}
				if (fos != null) {
					fos.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * 组装列
	 * @author liufeifei<br/>
	 * @param sswb
	 * @param sheet
	 * @param colNum
	 * @param rowNum
	 * @param content
	 * @param style
	 * @param isBorder
	 */
	public static void buildCellDataType(XSSFWorkbook sswb, Sheet sheet,
			int colNum, int rowNum, Object content, CellStyle style,
			boolean isBorder) {
		
		Row row = sheet.getRow(rowNum);
		if(row==null){
		   row = sheet.createRow(rowNum);
		}
		Cell cell = row.getCell(colNum) ;
		if(cell==null){
		   cell = row.createCell(colNum);
		}
		if (content != null) {
			if (content instanceof String) {
				cell.setCellStyle(style);
				cell.setCellValue(content.toString());
			} else if (content instanceof BigDecimal) {
				CellStyle cellStyle = sswb.createCellStyle();
				cellStyle.setFont(sswb.getFontAt(style.getFontIndex()));
				if (isBorder) {
					cellStyle.setBorderTop(HSSFCellStyle.BORDER_THIN);
					cellStyle.setBorderBottom(HSSFCellStyle.BORDER_THIN);
					cellStyle.setBorderLeft(HSSFCellStyle.BORDER_THIN);
					cellStyle.setBorderRight(HSSFCellStyle.BORDER_THIN);
				}
				cellStyle.setAlignment(HSSFCellStyle.ALIGN_RIGHT);
				cell.setCellStyle(cellStyle);
				cell.setCellValue(def.format((BigDecimal) content));
			} else if (content instanceof java.sql.Date) {
				cell.setCellStyle(style);
				cell.setCellValue(DateUtils.formatDate((java.sql.Date) content));
			} else if (content instanceof Integer) {
				CellStyle cellStyle = sswb.createCellStyle();
				cellStyle.setFont(sswb.getFontAt(style.getFontIndex()));
				if (isBorder) {
					cellStyle.setBorderTop(HSSFCellStyle.BORDER_THIN);
					cellStyle.setBorderBottom(HSSFCellStyle.BORDER_THIN);
					cellStyle.setBorderLeft(HSSFCellStyle.BORDER_THIN);
					cellStyle.setBorderRight(HSSFCellStyle.BORDER_THIN);
				}
				cellStyle.setAlignment(HSSFCellStyle.ALIGN_RIGHT);
				cell.setCellStyle(cellStyle);
				cell.setCellValue(Double.parseDouble(content.toString()));

			} else if (content instanceof Double) {
				CellStyle cellStyle = sswb.createCellStyle();
				cellStyle.setFont(sswb.getFontAt(style.getFontIndex()));
				if (isBorder) {
					cellStyle.setBorderTop(HSSFCellStyle.BORDER_THIN);
					cellStyle.setBorderBottom(HSSFCellStyle.BORDER_THIN);
					cellStyle.setBorderLeft(HSSFCellStyle.BORDER_THIN);
					cellStyle.setBorderRight(HSSFCellStyle.BORDER_THIN);
				}
				cellStyle.setAlignment(HSSFCellStyle.ALIGN_RIGHT);
				cell.setCellStyle(cellStyle);
				cell.setCellValue(def.format((Double) content));
			} else {
				cell.setCellStyle(style);
				cell.setCellValue(content.toString());
			}
		}
	}
}