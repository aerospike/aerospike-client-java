package com.ltv.aerospike.management.form;

import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.awt.FlowLayout;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.swing.AbstractAction;
import javax.swing.BorderFactory;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.KeyStroke;
import javax.swing.SwingUtilities;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.tree.DefaultMutableTreeNode;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rtextarea.RTextScrollPane;

import com.google.gson.Gson;
import com.ltv.aerospike.api.proto.ShowNamespaceServices.ShowNamespaceResponse;
import com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse;
import com.ltv.aerospike.client.AerospikeClient;
import com.ltv.aerospike.management.component.DefaultFont;
import com.ltv.aerospike.management.component.EditableTable;
import com.ltv.aerospike.management.component.EditableTree;
import com.ltv.aerospike.management.component.TableRenderer;
import com.ltv.aerospike.management.config.AppConstant;
import com.ltv.aerospike.management.config.NamespaceTreeCommand;
import com.ltv.aerospike.management.config.NamespaceTreeNode;
import com.ltv.aerospike.management.config.TableCommand;
import com.ltv.aerospike.management.event.MainEvent;
import com.ltv.aerospike.management.run.Aerospike;


public class  MainForm extends JFrame {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(MainForm.class.getSimpleName());
    public EditableTree treeNamespace = null;
    public RSyntaxTextArea areaQuery = new RSyntaxTextArea();
    public EditableTable queryTable = new EditableTable();
    public EditableTable structureTable = new EditableTable();
    public static MainForm instance = new MainForm();
    public JButton btnOpen = new JButton();
    public JButton btnSave = new JButton();
    public JButton btnRun = new JButton();
    public JButton btnLogout = new JButton();
    public JButton btnSetting = new JButton();
    public JLabel lblUser = new JLabel();
    public AerospikeClient aeClient = null;
    public NamespaceDialog namespaceDialog;
    public JDialog confirmDialog;
    public JDialog infoDialog;
    public SetDialog setDialog;
    public JLabel lblStatus = new JLabel();
    public JLabel lblOldCell = new JLabel();

    public JPopupMenu connectionTreePopup = new JPopupMenu();
    public JMenuItem createNamespaceMenu;
    public JMenuItem deleteNamespaceMenu;
    public JMenuItem editNamespaceMenu;
    public JMenuItem createSetMenu;
    public JMenuItem deleteSetMenu;
    public JMenuItem editSetMenu;
    public HashMap<String, Map> hashmapNamespace = new HashMap();
    public HashMap<String, Map> hashmapSet = new HashMap();
    public MainEvent mainEvent = new MainEvent();
    public boolean queryFlag = false;

    public static synchronized MainForm getInstance() {
        if(instance == null) instance = new MainForm();
        return instance;
    }

    public MainForm() {
        aeClient = Aerospike.aeConnector.get(LoginForm.getInstance().connectionName);
        buildFormInformation();

        // set layout
        setLayout(new BorderLayout(5, 5));
        JSplitPane splitPane = new JSplitPane();

        // fill layout
        splitPane.setLeftComponent(buildNavigatorPanel());
        splitPane.setRightComponent(buildWorkPanel());
        splitPane.setDividerLocation(300);
        add(splitPane, BorderLayout.CENTER);
        add(buildStatusBar(), BorderLayout.SOUTH);

        namespaceDialog = new NamespaceDialog(instance, AppConstant.NAMESPACE_INFORMATION, true);
        setDialog = new SetDialog(instance, AppConstant.SET_INFORMATION, true);
        confirmDialog = new JDialog(instance, AppConstant.CONFIRMATION, true);
        infoDialog = new JDialog(instance, AppConstant.INFORMATION, true);
    }

    public void loadSetDelay(Long delay) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                loadSet();
            }
        }, delay);
    }

    public void loadSet() {
        try {
            ShowNamespaceResponse response = aeClient.showNamespace();
            Map<String, String> mapNamespace = response.getNamespacesMap();
            mapNamespace.forEach((k,v) -> {
                Map<String, Object> mapBin = new Gson().fromJson(v, Map.class);
                hashmapNamespace.put(k, mapBin);
                ShowSetResponse setResponse = aeClient.showSet(k);
                Map<String, String> mapSet = setResponse.getSetsMap();
                mapSet.forEach((x,y) -> {
                    Map<String, Object> mapBinSet = new Gson().fromJson(y, Map.class);
                    hashmapSet.put(k + "|" + x, mapBinSet);
                });
            });
        } catch (Exception ex) {
            log.error("Get namespace tree node failed", ex);
        }
    }

    public void loadNamespaceDelay(Long delay) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                loadNamespace();
            }
        }, delay);
    }

    public void loadNamespace() {
        try {
            ShowNamespaceResponse response = aeClient.showNamespace();
            Map<String, String> mapNamespace = response.getNamespacesMap();
            mapNamespace.forEach((k,v) -> {
                Map<String, Object> mapBin = new Gson().fromJson(v, Map.class);
                hashmapNamespace.put(k, mapBin);
            });
        } catch (Exception ex) {
            log.error("Get namespace tree node failed", ex);
        }
    }

    public JPanel buildStatusBar() {
        JPanel statusBar = new JPanel();
        statusBar.setBorder(BorderFactory.createEmptyBorder(3, 5, 5, 3));
        statusBar.setLayout(new BorderLayout(5,5));
        statusBar.add(lblStatus, BorderLayout.WEST);
        statusBar.add(lblOldCell, BorderLayout.CENTER);
        lblOldCell.setVisible(false);
        DefaultFont.setFont(lblStatus);
        return statusBar;
    }

    private void buildFormInformation() {
        setTitle(AppConstant.APP_NAME);
        setSize(1024, 768);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocation((Toolkit.getDefaultToolkit().getScreenSize().width) / 2 - getWidth() / 2, (Toolkit.getDefaultToolkit().getScreenSize().height) / 2 - getHeight() / 2);
        setVisible(true);
    }

    public JSplitPane buildNavigatorPanel() {
        JSplitPane splitWorkPanel = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        splitWorkPanel.setResizeWeight(0.5);
        splitWorkPanel.setDividerLocation(0.5);
        splitWorkPanel.setTopComponent(buildNamespaceTree());
        splitWorkPanel.setBottomComponent(buildStructurePanel());
        return splitWorkPanel;
    }

    private JScrollPane buildNamespaceTree() {
        DefaultMutableTreeNode top = new DefaultMutableTreeNode(AppConstant.NAMESPACE);

        try {
            ShowNamespaceResponse response = aeClient.showNamespace();
            Map<String, String> mapNamespace = response.getNamespacesMap();
            mapNamespace.forEach((k,v) -> {
                Map<String, Object> mapBin = new Gson().fromJson(v, Map.class);
                hashmapNamespace.put(k, mapBin);
                DefaultMutableTreeNode nodeNamespace = new DefaultMutableTreeNode(k);
                top.add(nodeNamespace);
                ShowSetResponse setResponse = aeClient.showSet(k);
                Map<String, String> mapSet = setResponse.getSetsMap();
                mapSet.forEach((x,y) -> {
                    Map<String, Object> mapBinSet = new Gson().fromJson(y, Map.class);
                    hashmapSet.put(k + "|" + x, mapBinSet);
                    DefaultMutableTreeNode nodeSet = new DefaultMutableTreeNode(x);
                    nodeNamespace.add(nodeSet);
                });
            });
        } catch (Exception ex) {
            log.error("Get namespace tree node failed", ex);
        }

        treeNamespace = new EditableTree(top) {
            @Override
            public void buildPopupMenu() {
                createNamespaceMenu = buildMenu(AppConstant.CREATE_NAMESPACE, NamespaceTreeCommand.CREATE_NAMESPACE.getValue());
                deleteNamespaceMenu = buildMenu(AppConstant.DELETE_NAMESPACE, NamespaceTreeCommand.DELETE_NAMESPACE.getValue());
                editNamespaceMenu =  buildMenu(AppConstant.EDIT_NAMESPACE, NamespaceTreeCommand.EDIT_NAMESPACE.getValue());
                createSetMenu = buildMenu(AppConstant.CREATE_SET, NamespaceTreeCommand.CREATE_SET.getValue());
                deleteSetMenu = buildMenu(AppConstant.DELETE_SET, NamespaceTreeCommand.DELETE_SET.getValue());
                editSetMenu =  buildMenu(AppConstant.EDIT_SET, NamespaceTreeCommand.EDIT_SET.getValue());

                connectionTreePopup.add(createNamespaceMenu);
                connectionTreePopup.add(deleteNamespaceMenu);
                connectionTreePopup.add(editNamespaceMenu);
                connectionTreePopup.add(createSetMenu);
                connectionTreePopup.add(deleteSetMenu);
                connectionTreePopup.add(editSetMenu);

                addMouseListener(new MouseAdapter() {
                    public void mouseReleased(MouseEvent e) {
                        lblStatus.setText("");
                        mainEvent.namespaceTreeMouseListener(e);
                    }
                });
            }

            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                mainEvent.namespaceTreePopupListener(actionEvent);
            }
        };

        treeNamespace.addTreeSelectionListener(new TreeSelectionListener() {
            public void valueChanged(TreeSelectionEvent evt) {
                try {
                    lblStatus.setText("");
                    mainEvent.namespaceTreeSelectionChanged(evt);
                } catch (Exception ex) {
                    log.error("Get data failed:", ex);
                }
            }
        });

        JScrollPane scrollPane = new JScrollPane(treeNamespace);
        scrollPane.setViewportView(treeNamespace);
        scrollPane.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
        return scrollPane;
    }

    public void buildInformationDialog(JDialog dialog, String text) {
        buildDialog(dialog, "information.png", text, new JButton(AppConstant.OK) {{
            addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent actionEvent) {
                    dialog.setVisible(false);
                }
            });
        }}, null);
    }

    private void buildDialog(JDialog dialog, String image, String text, JButton okButton, JButton cancelButton) {
        JPanel confirmPanel = new JPanel();
        confirmPanel.setBorder(BorderFactory.createEmptyBorder(20, 20, 20, 20));
        confirmPanel.setLayout(new BorderLayout(5,5));
        JPanel textPanel = new JPanel();
        JPanel buttonPanel = new JPanel();
        confirmPanel.add(textPanel, BorderLayout.CENTER);
        confirmPanel.add(buttonPanel, BorderLayout.SOUTH);

        textPanel.setLayout(new BorderLayout(5,5));
        textPanel.add(new JLabel(new ImageIcon(getClass().getClassLoader().getResource(image))), BorderLayout.WEST);
        textPanel.add(new JLabel(text, JLabel.RIGHT), BorderLayout.CENTER);

        buttonPanel.setLayout(new FlowLayout(FlowLayout.CENTER));
        buttonPanel.add(okButton);
        if(cancelButton != null) buttonPanel.add(cancelButton);

        dialog.getContentPane().add(confirmPanel);
        dialog.setLocationRelativeTo(null);
        dialog.pack();
        dialog.setVisible(true);
    }

    public void buildConfirmDialog(JDialog dialog, String text, JButton okButton, JButton cancelButton) {
        buildDialog(dialog, "question.png", text, okButton, cancelButton);
    }

    public void showNamespaceDialog() {
        EventQueue.invokeLater(() -> {
            namespaceDialog.cboUser.removeAllItems();
            namespaceDialog.lstRole.removeAll(namespaceDialog.lstRole);
            DefaultTableModel dtm = (DefaultTableModel) namespaceDialog.tblRole.getModel();
            dtm.setRowCount(0);
            namespaceDialog.loadNamespaceForm();
        });
    }

    public void showSetDialog() {
        EventQueue.invokeLater(() -> {
            setDialog.cboUser.removeAllItems();
            setDialog.lstPermission.removeAll(setDialog.lstPermission);
            DefaultTableModel dtm = (DefaultTableModel) setDialog.tblPermission.getModel();
            dtm.setRowCount(0);
            setDialog.loadSetForm();
        });
    }

    public JSplitPane buildWorkPanel() {
        JSplitPane splitWorkPanel = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        splitWorkPanel.setDividerLocation(500);
        splitWorkPanel.setTopComponent(buildQueryPanel());
        splitWorkPanel.setBottomComponent(buildTablePanel());
        return splitWorkPanel;
    }

    public JPanel buildQueryPanel() {
        JPanel queryPanel = new JPanel();
        queryPanel.setLayout(new BorderLayout(5,5));
        queryPanel.add(buildButtonPanel(), BorderLayout.NORTH);
        queryPanel.add(BuildTextPanel(), BorderLayout.CENTER);
        return queryPanel;
    }

    public JPanel buildButtonPanel() {
        JPanel buttonPanel = new JPanel();
        buttonPanel.setLayout(new BorderLayout(5, 5));
        JPanel leftPanel = new JPanel();
        buttonPanel.add(leftPanel, BorderLayout.CENTER);
        JPanel rightPanel = new JPanel();
        rightPanel.setLayout(new FlowLayout());
        DefaultFont.setFont(lblUser);
        rightPanel.add(lblUser);
        //rightPanel.add(createButton(btnSetting, "setting.png", AppConstant.SETTING));
        rightPanel.add(createButton(btnLogout, "logout.png", AppConstant.LOGOUT));
        addLogoutEvent();
        buttonPanel.add(rightPanel, BorderLayout.EAST);

        leftPanel.setLayout(new FlowLayout(FlowLayout.LEFT));
        //leftPanel.add(createButton(btnOpen, "folder.png", AppConstant.OPEN));
        //leftPanel.add(createButton(btnSave, "save.png", AppConstant.SAVE));
        leftPanel.add(createButton(btnRun, "run.png", AppConstant.RUN));
        btnRun.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                try {
                    mainEvent.runQuery();
                } catch (Exception ex) {
                    log.info("Run query failed!", ex);
                }
            }
        });

        return buttonPanel;
    }

    private void addLogoutEvent() {
        btnLogout.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                try {
                    aeClient.logout();
                    setVisible(false);
                    instance = null;
                    LoginForm.getInstance().setVisible(true);
                    dispose();
                } catch (Exception ex) {
                    log.info("login failed!", ex);
                }
            }
        });
    }

    private JButton createButton(JButton button, String image, String tooltip) {
        button.setIcon(new ImageIcon(getClass().getClassLoader().getResource(image)));
        button.setHorizontalTextPosition(JButton.CENTER);
        button.setVerticalTextPosition(JButton.BOTTOM);
        button.setToolTipText(tooltip);
        button.setBorder(BorderFactory.createCompoundBorder(
                button.getBorder(),
                BorderFactory.createEmptyBorder(1, 1, 1, 1)));
        return button;
    }

    public JPanel BuildTextPanel() {
        JPanel panelText = new JPanel();
        panelText.setLayout(new BorderLayout(0, 0));
        RTextScrollPane rscrollPane = new RTextScrollPane(areaQuery);
        areaQuery.addKeyListener(new KeyListener(){
            @Override
            public void keyTyped(KeyEvent e) {
            }

            @Override
            public void keyPressed(KeyEvent e) {
                if(e.getKeyCode() == KeyEvent.VK_F9) {
                    mainEvent.runQuery();
                }
            }

            @Override
            public void keyReleased(KeyEvent e) {
            }

        });

        KeyStroke controlR = KeyStroke.getKeyStroke("control R");
        String ACTION_KEY = "Control-R";
        areaQuery.getInputMap(JComponent.WHEN_FOCUSED).put(controlR, ACTION_KEY);
        areaQuery.getActionMap().put(ACTION_KEY, new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                try {
                    mainEvent.runQuery();
                } catch (Exception ex) {
                    log.info("Run query failed!", ex);
                }
            }
        });

        rscrollPane.setViewportView(areaQuery);
        panelText.add(rscrollPane, BorderLayout.CENTER);
        areaQuery.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_SQL);
        areaQuery.setCodeFoldingEnabled(true);
        areaQuery.setEditable(true);
        areaQuery.setLineWrap(false);
        areaQuery.setWrapStyleWord(false);
        areaQuery.setAnimateBracketMatching(false);
        areaQuery.setAutoIndentEnabled(false);
        areaQuery.setBracketMatchingEnabled(false);
        return panelText;
    }

    public JScrollPane buildStructurePanel() {
        structureTable = new EditableTable() {
            @Override
            public void buildPopupMenu() {
                JPopupMenu popup = new JPopupMenu();
                JMenuItem queryAddMenu = buildMenu(AppConstant.ADD, TableCommand.ADD.getValue());
                JMenuItem queryDeleteMenu = buildMenu(AppConstant.DELETE, TableCommand.DELETE.getValue());
                popup.add(queryAddMenu);
                popup.add(queryDeleteMenu);

                addMouseListener(new MouseAdapter() {
                    public void mouseReleased(MouseEvent e) {
                    showTablePopupMenu(e, popup);
                    }
                });
                getTableHeader().addMouseListener(new MouseAdapter() {
                    @Override
                    public void mouseReleased(MouseEvent e) {
                    showTablePopupMenu(e, popup);
                    }
                });
                DefaultFont.setFont(popup);
            }

            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                if(treeNamespace.getSelectionPath().getPathCount() != 3) return;
                switch (actionEvent.getActionCommand()) {
                    case TableCommand.ADD_VALUE:
                        structureTable.addNewRow();
                        int lastRow = structureTable.getModel().getRowCount() - 1;
                        structureTable.setRowSelectionInterval(lastRow, lastRow);
                        break;
                    case TableCommand.DELETE_VALUE:
                        mainEvent.structureTableOnDelete();
                        break;
                }
            }
        };

        structureTable.addPropertyChangeListener(new PropertyChangeListener() {
            @Override
            public void propertyChange(PropertyChangeEvent propertyChangeEvent) {
                if ("tableCellEditor".equals(propertyChangeEvent.getPropertyName()))
                {
                    // if finished editing
                    String selectedValue = "";
                    if(structureTable.getSelectedRow() == -1 ) return;
                    if(structureTable.getSelectedColumn() == -1 ) return;
                    if(structureTable.getValueAt(structureTable.getSelectedRow(), structureTable.getSelectedColumn()) != null)
                            selectedValue = structureTable.getValueAt(structureTable.getSelectedRow(), structureTable.getSelectedColumn()).toString();
                    if (structureTable.isEditing()) {
                        lblStatus.setText("Table row is editing");
                        lblOldCell.setText(selectedValue);
                    } else {
                        if (!lblOldCell.getText().equals(selectedValue)) {
                            String binName = structureTable.getColumnName(structureTable.getSelectedColumn());
                            mainEvent.saveStructureTableRow(binName, lblOldCell.getText(), selectedValue);
                            loadSet();
                            mainEvent.setOnClick();
                        }
                        lblStatus.setText("Table row saved");
                    }
                }
            }
        });
        
        structureTable.setDefaultRenderer(Object.class, new TableRenderer());
        JScrollPane structurePanel = new JScrollPane(structureTable);
        structurePanel.setViewportView(structureTable);
        return structurePanel;
    }

    public JScrollPane buildTablePanel() {
        queryTable = new EditableTable() {
            @Override
            public void buildPopupMenu() {
                JPopupMenu popup = new JPopupMenu();
                JMenuItem queryAddMenu = buildMenu(AppConstant.ADD, TableCommand.ADD.getValue());
                JMenuItem queryDeleteMenu = buildMenu(AppConstant.DELETE, TableCommand.DELETE.getValue());
                popup.add(queryAddMenu);
                popup.add(queryDeleteMenu);

                addMouseListener(new MouseAdapter() {
                    public void mouseReleased(MouseEvent e) {
                        showTablePopupMenu(e, popup);
                    }
                });
                getTableHeader().addMouseListener(new MouseAdapter() {
                    @Override
                    public void mouseReleased(MouseEvent e) {
                        showTablePopupMenu(e, popup);
                    }
                });
                DefaultFont.setFont(popup);
            }

            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                if(treeNamespace.getSelectionPath().getPathCount() != 3) return;
                switch (actionEvent.getActionCommand()) {
                    case TableCommand.ADD_VALUE:
                        queryTable.addNewRow();
                        int lastRow = queryTable.getModel().getRowCount() - 1;
                        queryTable.setRowSelectionInterval(lastRow, lastRow);
                        break;
                    case TableCommand.DELETE_VALUE:
                        mainEvent.queryTableOnDelete();
                        break;
                }
            }
        };

        queryTable.addPropertyChangeListener(new PropertyChangeListener() {
            @Override
            public void propertyChange(PropertyChangeEvent propertyChangeEvent) {
                if ("tableCellEditor".equals(propertyChangeEvent.getPropertyName()))
                {
                    // if finished editing
                    String selectedValue = "";
                    if(queryTable.getSelectedRow() == -1 || queryTable.getSelectedColumn() == -1) return;
                    if(queryTable.getValueAt(queryTable.getSelectedRow(), queryTable.getSelectedColumn()) != null)
                        selectedValue = queryTable.getValueAt(queryTable.getSelectedRow(), queryTable.getSelectedColumn()).toString();
                    if (queryTable.isEditing()) {
                        lblStatus.setText("Table row is editing");
                        lblOldCell.setText(selectedValue);
                    } else {
                        Object key = queryTable.getValueAt(queryTable.getSelectedRow(), getInstance().queryTable.getColumn(AppConstant.KEY).getModelIndex());
                        if(key == null || key.toString().isEmpty()) {
                            queryTable.setValueAt(lblOldCell.getText(), queryTable.getSelectedRow(), queryTable.getSelectedColumn());
                            lblStatus.setText("Id is required");
                            return;
                        }

                        if(!lblOldCell.getText().equals(selectedValue)) {
                            String binName = queryTable.getColumnName(queryTable.getSelectedColumn());
                            mainEvent.saveQueryTableRow(key.toString(), binName, selectedValue, lblOldCell.getText());
                        }
                        lblStatus.setText("Table row saved");
                    }
                }
            }
        });
        
        queryTable.setDefaultRenderer(Object.class, new TableRenderer());
        JScrollPane tablePanel = new JScrollPane(queryTable);
        tablePanel.setViewportView(queryTable);
        return tablePanel;
    }

    private void showTablePopupMenu(MouseEvent e, JPopupMenu popup) {
        if (SwingUtilities.isRightMouseButton(e)) {
            if(treeNamespace.getSelectionPath() != null) {
                switch (treeNamespace.getSelectionPath().getPathCount()) {
                    case NamespaceTreeNode.SET_VALUE:
                        popup.show((JComponent) e.getSource(), e.getX(), e.getY());
                        break;
                }
            }
        }
    }
}
