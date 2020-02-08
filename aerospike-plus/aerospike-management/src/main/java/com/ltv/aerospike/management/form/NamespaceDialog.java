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
import com.ltv.aerospike.management.event.NamespaceEvent;

import io.grpc.stub.StreamObserver;

public class NamespaceDialog extends JDialog {
    public JTextField txtNamespace = new JTextField();
    public JLabel txtOldNamespace = new JLabel();
    public JComboBox cboUser = new JComboBox();
    public JComboBox cboRole = new JComboBox(new String[]{AppConstant.ROLE_OWNER,AppConstant.ROLE_DDL,AppConstant.ROLE_DQL,AppConstant.ROLE_DML,AppConstant.ROLE_DCL});
    public JButton btnAdd = new JButton(AppConstant.ADD);
    public JButton btnSave = new JButton(AppConstant.SAVE);
    public EditableTable tblRole;
    public List<List> lstRole = new ArrayList();
    public NamespaceEvent namespaceEvent = new NamespaceEvent();

    public NamespaceDialog(Frame owner, String title, boolean modal) {
        super(owner, title, modal);
        JPanel namespacePanel = new JPanel();
        namespacePanel.setBorder(BorderFactory.createEmptyBorder(20, 20, 20, 20));
        getContentPane().add(namespacePanel);
        namespacePanel.setLayout(new BorderLayout(5, 5));
        JPanel namePanel = new JPanel();
        JPanel rolePanel = new JPanel();
        namespacePanel.add(namePanel, BorderLayout.NORTH);
        namespacePanel.add(rolePanel, BorderLayout.CENTER);
        namespacePanel.add(btnSave, BorderLayout.SOUTH);

        namePanel.setLayout(new BorderLayout(5,5));
        namePanel.add(new JLabel(AppConstant.NAMESPACE), BorderLayout.WEST);
        namePanel.add(txtNamespace, BorderLayout.CENTER);
        txtOldNamespace.setVisible(false);
        namePanel.add(txtOldNamespace, BorderLayout.EAST);

        JPanel searchUserPanel = new JPanel();
        searchUserPanel.setLayout(new GridLayout(1,5));
        searchUserPanel.add(new JLabel(AppConstant.USER_NAME, SwingConstants.RIGHT));
        cboUser.setRenderer(new ItemRenderer());
        searchUserPanel.add(cboUser);
        searchUserPanel.add(new JLabel(AppConstant.ROLE, SwingConstants.RIGHT));
        searchUserPanel.add(cboRole);

        searchUserPanel.add(btnAdd, BorderLayout.EAST);
        rolePanel.setLayout(new BorderLayout(5,5));
        rolePanel.add(searchUserPanel, BorderLayout.NORTH);
        tblRole = new EditableTable() {
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
                switch (actionEvent.getActionCommand()) {
                    case TableCommand.DELETE_VALUE:
                        HashMap mapUser = new HashMap();
                        for(int i = 0; i < cboUser.getItemCount(); i++) {
                            Item item = (Item) cboUser.getItemAt(i);
                            mapUser.put(item.getId(), item.getDescription());
                        }
                        for(List row : lstRole) {
                            if(mapUser.get(row.get(0)).equals(tblRole.getValueAt(tblRole.getSelectedRow(), 0))
                               && row.get(1).equals(tblRole.getValueAt(tblRole.getSelectedRow(), 1))
                            ) {
                                lstRole.remove(row);
                                break;
                            }
                        }
                        tblRole.removeSelectedRows();
                        break;
                }
            }
        };
        rolePanel.add(tblRole, BorderLayout.CENTER);
        String[] header = {"User","Role"};
        tblRole.setModel(new DefaultTableModel(header, 0));
        rolePanel.setBorder(BorderFactory.createTitledBorder(AppConstant.PERMISSION));
        setSize(new Dimension(600, 400));
        setLocation((Toolkit.getDefaultToolkit().getScreenSize().width) / 2 - 300, (Toolkit.getDefaultToolkit().getScreenSize().height) / 2 - 200);
        setResizable(false);

        btnAdd.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                namespaceEvent.addRoleButtonClick(e);
            }
        });

        btnSave.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                if(txtNamespace.getText().trim().isEmpty()) {
                    MainForm.getInstance().lblStatus.setText("Namespace is required");
                    return;
                }
                if(!txtOldNamespace.getText().trim().isEmpty()) {
                    namespaceEvent.renameNamespace(txtOldNamespace.getText(), txtNamespace.getText());
                }
                namespaceEvent.saveNamespaceButtonClick(e);
            }
        });
    }

    public void loadNamespaceForm() {
        List<Map<String, Object>> lstUser = new ArrayList();
        MainForm.getInstance().aeClient.query(new StreamObserver<QueryResponse>() {
            @Override
            public void onNext(QueryResponse queryResponse) {
                Map user = MainForm.getInstance().aeClient.parseRecord(queryResponse);
                if(user.get(AppConstant.KEY) != null) lstUser.add(user);
            }

            @Override
            public void onError(Throwable throwable) {
                MainForm.getInstance().lblStatus.setText("Namespace data loaded failed");
            }

            @Override
            public void onCompleted() {
                SwingUtilities.invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        if(lstUser.isEmpty()) return;
                        Map<String, String> mapUser = new HashMap();
                        for(Map<String, Object> record : lstUser) {
                            MainForm.getInstance().namespaceDialog.cboUser.addItem(new Item((String)record.get(AppConstant.KEY), (String)record.get("name")));
                            mapUser.put(record.get(AppConstant.KEY).toString(), record.get("name").toString());
                        }
                        namespaceEvent.loadTblRole(mapUser);
                        MainForm.getInstance().namespaceDialog.setVisible(true);
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
