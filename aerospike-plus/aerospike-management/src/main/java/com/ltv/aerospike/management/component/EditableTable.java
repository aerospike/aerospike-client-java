package com.ltv.aerospike.management.component;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JMenuItem;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

public class EditableTable extends JTable implements ActionListener {

    public EditableTable() {
        setDefaultRenderer(Object.class, new TableRenderer());
        getTableHeader().setDefaultRenderer(new HeaderTableRenderer());
        setRowHeight(20);
        buildPopupMenu();
        DefaultFont.setFont(this);
    }

    public void buildPopupMenu() {

    }

    public void removeSelectedRows(){
        DefaultTableModel model = (DefaultTableModel) getModel();
        int[] rows = getSelectedRows();
        for(int i=0;i<rows.length;i++){
            model.removeRow(rows[i]-i);
        }
    }

    public void addNewRow() {
        DefaultTableModel model = (DefaultTableModel) getModel();
        String[] row = new String[model.getColumnCount()];
        for(int i = 0; i < row.length; i++) {
            row[i] = "";
        }
        model.addRow(row);
    }

    public JMenuItem buildMenu(String text, String command) {
        JMenuItem item = new JMenuItem(text);
        item.addActionListener(this);
        item.setActionCommand(command);
        DefaultFont.setFont(item);
        return item;
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent) {
    }
}
