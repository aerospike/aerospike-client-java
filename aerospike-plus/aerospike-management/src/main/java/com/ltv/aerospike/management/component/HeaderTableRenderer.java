package com.ltv.aerospike.management.component;

import java.awt.Color;
import java.awt.Component;

import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

public class HeaderTableRenderer extends DefaultTableCellRenderer {
    @Override
    public Component getTableCellRendererComponent(JTable table, Object value,
                                                   boolean isSelected, boolean hasFocus, int row, int column) {
        setBackground(Color.lightGray);
        return super.getTableCellRendererComponent(table,value,isSelected,hasFocus,row,column);
    }



}