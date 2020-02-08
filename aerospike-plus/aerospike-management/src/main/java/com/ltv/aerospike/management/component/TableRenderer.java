package com.ltv.aerospike.management.component;

import java.awt.Color;
import java.awt.Component;

import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

public class TableRenderer extends DefaultTableCellRenderer {
    @Override
    public Component getTableCellRendererComponent(JTable table, Object value,
                                                   boolean isSelected, boolean hasFocus, int row, int column) {
        if(!isSelected ) {
            Color c = table.getBackground();
            if( (row%2)==0 &&
                c.getRed()>10 && c.getGreen()>10 && c.getBlue()>10 )
                setBackground(new Color( c.getRed()-10,
                                         c.getGreen()-10,
                                         c.getBlue()-10));
            else
                setBackground(c);
        }
        return super.getTableCellRendererComponent(table,value,isSelected,hasFocus,row,column);
    }



}