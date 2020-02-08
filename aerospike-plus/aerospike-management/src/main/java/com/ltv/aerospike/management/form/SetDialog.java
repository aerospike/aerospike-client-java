package com.ltv.aerospike.management.form;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridLayout;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.plaf.basic.BasicComboBoxRenderer;
import javax.swing.table.DefaultTableModel;

import com.ltv.aerospike.api.proto.QueryServices.QueryResponse;
import com.ltv.aerospike.management.component.DefaultFont;
import com.ltv.aerospike.management.component.EditableTable;
import com.ltv.aerospike.management.config.AppConstant;
import com.ltv.aerospike.management.config.TableCommand;
import com.ltv.aerospike.management.event.SetEvent;

import io.grpc.stub.StreamObserver;

public class SetDialog extends JDialog {
    public JTextField txtSet = new JTextField();
    public JLabel txtOldSet = new JLabel();
    public JComboBox cboUser = new JComboBox();
    public JComboBox cboPermission = new JComboBox(new String[]{ AppConstant.PERMISSION_SELECT, AppConstant.PERMISSION_PUT, AppConstant.PERMISSION_DELETE});
    public JButton btnAdd = new JButton(AppConstant.ADD);
    public JButton btnSave = new JButton(AppConstant.SAVE);
    public EditableTable tblPermission;
    public List<List> lstPermission = new ArrayList();
    public SetEvent setEvent = new SetEvent();

    public SetDialog(Frame owner, String title, boolean modal) {
        super(owner, title, modal);
        JPanel setPanel = new JPanel();
        setPanel.setBorder(BorderFactory.createEmptyBorder(20, 20, 20, 20));
        getContentPane().add(setPanel);
        setPanel.setLayout(new BorderLayout(5, 5));
        JPanel namePanel = new JPanel();
        JPanel rolePanel = new JPanel();
        setPanel.add(namePanel, BorderLayout.NORTH);
        setPanel.add(rolePanel, BorderLayout.CENTER);
        setPanel.add(btnSave, BorderLayout.SOUTH);

        namePanel.setLayout(new BorderLayout(5,5));
        namePanel.add(new JLabel(AppConstant.SET), BorderLayout.WEST);
        namePanel.add(txtSet, BorderLayout.CENTER);
        txtOldSet.setVisible(false);
        namePanel.add(txtOldSet, BorderLayout.EAST);

        JPanel searchUserPanel = new JPanel();
        searchUserPanel.setLayout(new GridLayout(1,5));
        searchUserPanel.add(new JLabel(AppConstant.USER_NAME, SwingConstants.RIGHT));
        cboUser.setRenderer(new ItemRenderer());
        searchUserPanel.add(cboUser);
        searchUserPanel.add(new JLabel(AppConstant.ROLE, SwingConstants.RIGHT));
        searchUserPanel.add(cboPermission);

        searchUserPanel.add(btnAdd, BorderLayout.EAST);
        rolePanel.setLayout(new BorderLayout(5,5));
        rolePanel.add(searchUserPanel, BorderLayout.NORTH);
        tblPermission = new EditableTable() {
            @Override
            public void buildPopupMenu() {
                JPopupMenu popup = new JPopupMenu();
                popup.add(buildMenu(AppConstant.DELETE, TableCommand.DELETE.getValue()));
                addMouseListener(new MouseAdapter() {
                    public void mouseReleased(MouseEvent e) {
                        if (SwingUtilities.isRightMouseButton(e)) {
                            popup.show((JComponent) e.getSource(), e.getX(), e.getY());
                        }
                    }
                });
                getTableHeader().addMouseListener(new MouseAdapter() {
                    @Override
                    public void mouseReleased(MouseEvent e) {
                        if (SwingUtilities.isRightMouseButton(e)) {
                            popup.show((JComponent) e.getSource(), e.getX(), e.getY());
                        }
                    }
                });
                DefaultFont.setFont(popup);
            }

            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                if(tblPermission.getSelectedRow() == -1) return;
                switch (actionEvent.getActionCommand()) {
                    case TableCommand.DELETE_VALUE:
                        HashMap mapUser = new HashMap();
                        for(int i = 0; i < cboUser.getItemCount(); i++) {
                            Item item = (Item) cboUser.getItemAt(i);
                            mapUser.put(item.getId(), item.getDescription());
                        }
                        for(List row : lstPermission) {
                            if(mapUser.get(row.get(0)).equals(tblPermission.getValueAt(tblPermission.getSelectedRow(), 0))
                               && row.get(1).equals(tblPermission.getValueAt(tblPermission.getSelectedRow(), 1))
                            ) {
                                lstPermission.remove(row);
                                break;
                            }
                        }
                        tblPermission.removeSelectedRows();
                        break;
                }
            }
        };

        rolePanel.add(tblPermission, BorderLayout.CENTER);
        String[] header = {"User","Permission"};
        tblPermission.setModel(new DefaultTableModel(header, 0));
        rolePanel.setBorder(BorderFactory.createTitledBorder(AppConstant.PERMISSION));
        setSize(new Dimension(600, 400));
        setLocation((Toolkit.getDefaultToolkit().getScreenSize().width) / 2 - 300, (Toolkit.getDefaultToolkit().getScreenSize().height) / 2 - 200);
        setResizable(false);

        btnAdd.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                setEvent.addRoleButtonClick(e);
            }
        });

        btnSave.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                if(txtSet.getText().trim().isEmpty()) {
                    MainForm.getInstance().lblStatus.setText("Set is required");
                    return;
                }
                setEvent.saveSetButtonClick(e);
            }
        });
    }

    public void loadSetForm() {
        List<Map<String, Object>> lstUser = new ArrayList();
        MainForm.getInstance().aeClient.query(new StreamObserver<QueryResponse>() {
            @Override
            public void onNext(QueryResponse queryResponse) {
                Map user = MainForm.getInstance().aeClient.parseRecord(queryResponse);
                if(user.get(AppConstant.KEY) != null) lstUser.add(user);
            }

            @Override
            public void onError(Throwable throwable) {
                MainForm.getInstance().lblStatus.setText("Set data loaded failed");
            }

            @Override
            public void onCompleted() {
                SwingUtilities.invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        Map<String, String> mapUser = new HashMap();
                        for(Map<String, Object> record : lstUser) {
                            MainForm.getInstance().setDialog.cboUser.addItem(new Item((String)record.get(AppConstant.KEY), (String)record.get("name")));
                            mapUser.put(record.get(AppConstant.KEY).toString(), record.get("name").toString());
                        }

                        setEvent.loadTblPermission(mapUser);
                        MainForm.getInstance().setDialog.setVisible(true);
                    }
                });
            }
        }, "aerospike", "user");
    }

    class ItemRenderer extends BasicComboBoxRenderer
    {
        public Component getListCellRendererComponent( JList list, Object value, int index, boolean isSelected, boolean cellHasFocus)
        {
            super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);

            if (value != null)
            {
                Item item = (Item)value;
                setText( item.getDescription() );
            }

            return this;
        }
    }

    public class Item
    {
        private String id;
        private String description;

        public Item(String id, String description)
        {
            this.id = id;
            this.description = description;
        }

        public String getId()
        {
            return id;
        }

        public String getDescription()
        {
            return description;
        }

        public String toString()
        {
            return description;
        }
    }
}
