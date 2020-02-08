package com.ltv.aerospike.management.component;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JMenuItem;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;

public class EditableTree extends JTree implements ActionListener {

    public EditableTree(DefaultMutableTreeNode root) {
        super(root);
        buildPopupMenu();
        setEditable(false);
        DefaultFont.setFont(this);
    }

    public void buildPopupMenu() {

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
