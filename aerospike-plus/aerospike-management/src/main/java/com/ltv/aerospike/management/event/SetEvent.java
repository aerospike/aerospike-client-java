package com.ltv.aerospike.management.event;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.SwingUtilities;
import javax.swing.table.DefaultTableModel;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import com.ltv.aerospike.api.proto.CreateSetServices.CreateSetResponse;
import com.ltv.aerospike.api.proto.DropSetServices.DropSetResponse;
import com.ltv.aerospike.api.proto.QueryServices.QueryRequest.FilterOperation;
import com.ltv.aerospike.api.proto.QueryServices.QueryResponse;
import com.ltv.aerospike.api.proto.RenameSetServices.RenameSetResponse;
import com.ltv.aerospike.client.Qualifier;
import com.ltv.aerospike.management.config.AppConstant;
import com.ltv.aerospike.management.form.MainForm;
import com.ltv.aerospike.management.form.SetDialog.Item;

import io.grpc.stub.StreamObserver;

public class SetEvent {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(SetEvent.class.getSimpleName());
    public void addRoleButtonClick(ActionEvent e) {
        Item userItem = (Item)MainForm.getInstance().setDialog.cboUser.getSelectedItem();
        if(userItem == null) return;
        String user = userItem.getDescription();
        String permission = MainForm.getInstance().setDialog.cboPermission.getSelectedItem().toString();
        DefaultTableModel model = (DefaultTableModel) MainForm.getInstance().setDialog.tblPermission.getModel();
        Vector<Vector> data = model.getDataVector();
        for(Vector row : data) {
            if(row.get(0).equals(user) && row.get(1).equals(permission)) return;
        };

        String[] row = new String[2];
        row[0] = user;
        row[1] = permission;
        model.addRow(row);
        List lstRow = new ArrayList();
        lstRow.add(userItem.getId());
        lstRow.add(permission);
        MainForm.getInstance().setDialog.lstPermission.add(lstRow);
    }

    public void saveSetButtonClick(ActionEvent e) {
        String set = MainForm.getInstance().setDialog.txtSet.getText();
        HashMap<String, String> setInfo = new HashMap();

        StringBuilder selectPermission = new StringBuilder();
        StringBuilder putPermission = new StringBuilder();
        StringBuilder deletePermission = new StringBuilder();
        MainForm.getInstance().setDialog.lstPermission.forEach((row) -> {
            switch ((String)row.get(1)) {
                case AppConstant.PERMISSION_PUT:
                    putPermission.append(row.get(0));
                    putPermission.append(",");
                    break;
                case AppConstant.PERMISSION_DELETE:
                    deletePermission.append(row.get(0));
                    deletePermission.append(",");
                    break;
                case AppConstant.PERMISSION_SELECT:
                    selectPermission.append(row.get(0));
                    selectPermission.append(",");
                    break;
            }
        });

        setInfo.put(AppConstant.PERMISSION_SELECT, "");
        setInfo.put(AppConstant.PERMISSION_PUT, "");
        setInfo.put(AppConstant.PERMISSION_DELETE, "");

        if (selectPermission.length() > 0) {
            selectPermission.setLength(selectPermission.length() - 1);
            setInfo.put(AppConstant.PERMISSION_SELECT, selectPermission.toString());
        }
        if (putPermission.length() > 0) {
            putPermission.setLength(putPermission.length() - 1);
            setInfo.put(AppConstant.PERMISSION_PUT, putPermission.toString());
        }
        if (deletePermission.length() > 0) {
            deletePermission.setLength(deletePermission.length() - 1);
            setInfo.put(AppConstant.PERMISSION_DELETE, deletePermission.toString());
        }

        String oldSet = MainForm.getInstance().setDialog.txtOldSet.getText();
        if(!oldSet.trim().isEmpty() && !oldSet.equals(set)) {
            setInfo.put("name", set);
            createSet(oldSet, setInfo);
        } else {
            createSet(set, setInfo);
        }
    }

    public void createSet(String set, HashMap<String, String> setInfo) {
        String namespace = MainForm.getInstance().treeNamespace.getSelectionPath().getPath()[1].toString();
        List<Integer> lstResult = new ArrayList();
        MainForm.getInstance().aeClient.createSet(new StreamObserver<CreateSetResponse>() {
            @Override
            public void onNext(CreateSetResponse createSetResponse) {
                lstResult.add(createSetResponse.getSetId());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                try {
                    MainForm.getInstance().loadSetDelay(1000l);
                    MainForm.getInstance().lblStatus.setText("Create set successfully");
                    saveUpdateUI(set, set);
                } catch (Exception ex) {
                    log.error("Create set failed", ex);
                }
            }
        }, namespace, set, setInfo);
    }

    public void saveUpdateUI(String set, String oldSet) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                MainForm.getInstance().setDialog.setVisible(false);
                TreePath path = MainForm.getInstance().treeNamespace.getSelectionPath();
                if(!MainForm.getInstance().setDialog.txtOldSet.getText().isEmpty()) {
                    DefaultTreeModel model = (DefaultTreeModel) MainForm.getInstance().treeNamespace.getModel();
                    DefaultMutableTreeNode node = (DefaultMutableTreeNode) path.getLastPathComponent();
                    node.setUserObject(MainForm.getInstance().setDialog.txtSet.getText());
                    model.nodeChanged(node);
                    if(!set.equals(oldSet)) {
                        MainForm.getInstance().hashmapSet.remove(oldSet);
                        MainForm.getInstance().loadSet();
                    }
                } else {
                    DefaultMutableTreeNode root = (DefaultMutableTreeNode) path.getPathComponent(1);
                    root.add(new DefaultMutableTreeNode(set));
                    ((DefaultTreeModel) MainForm.getInstance().treeNamespace.getModel()).nodeStructureChanged(root);
                }

                MainForm.getInstance().lblStatus.setText("Set saved successfully");
            }
        });
    }

    public void deleteSet() {
        MainForm.getInstance().buildConfirmDialog(MainForm.getInstance().confirmDialog, AppConstant.CONFIRM_DELETE,
              new JButton(AppConstant.OK) {{
                  addActionListener(new ActionListener() {
                      @Override
                      public void actionPerformed(ActionEvent actionEvent) {
                          MainForm.getInstance().aeClient.dropSet(
                                  new StreamObserver<DropSetResponse>() {
                                      @Override
                                      public void onNext(DropSetResponse dropSetResponse) {
                                      }

                                      @Override
                                      public void onError(Throwable throwable) {
                                          MainForm.getInstance().lblStatus.setText("Set deleted failed");
                                      }

                                      @Override
                                      public void onCompleted() {
                                          SwingUtilities.invokeLater(new Runnable() {
                                              @Override
                                              public void run() {
                                                  DefaultMutableTreeNode selectedNode = (DefaultMutableTreeNode)MainForm.getInstance().treeNamespace.getLastSelectedPathComponent();
                                                  DefaultMutableTreeNode parentNode = (DefaultMutableTreeNode) selectedNode.getParent();
                                                  int nodeIndex = parentNode.getIndex(selectedNode);
                                                  selectedNode.removeAllChildren();
                                                  parentNode.remove(nodeIndex);
                                                  ((DefaultTreeModel) MainForm.getInstance().treeNamespace.getModel()).nodeStructureChanged(selectedNode);
                                                  MainForm.getInstance().confirmDialog.setVisible(false);
                                                  MainForm.getInstance().lblStatus.setText("Set deleted successfully");
                                                  MainForm.getInstance().loadSetDelay(1000l);
                                              }
                                          });
                                      }
                                  },
                                  MainForm.getInstance().treeNamespace.getSelectionPath().getPath()[1].toString(),
                                  MainForm.getInstance().treeNamespace.getLastSelectedPathComponent().toString());
                      }
                  });
              }},
              new JButton(AppConstant.CANCEL) {{
                  addActionListener(new ActionListener() {
                      @Override
                      public void actionPerformed(ActionEvent actionEvent) {
                          MainForm.getInstance().confirmDialog.setVisible(false);
                      }
                  });
              }});
    }

    public void loadTblPermission(Map<String, String> mapUser) {
        String set = MainForm.getInstance().setDialog.txtSet.getText();
        if(set != null && !set.isEmpty()) {
            List<Map<String, Object>> lstSet = new ArrayList();
            MainForm.getInstance().aeClient.query(new StreamObserver<QueryResponse>() {
                @Override
                public void onNext(QueryResponse response) {
                    lstSet.add(MainForm.getInstance().aeClient.parseRecord(response));
                }

                @Override
                public void onError(Throwable throwable) {
                    MainForm.getInstance().lblStatus.setText("Can't get set role");
                }

                @Override
                public void onCompleted() {
                    Map<String, Object> data = lstSet.get(0);
                    List<List> lstPermission = MainEvent.importRole(new ArrayList(), AppConstant.PERMISSION_SELECT, (String) data.get(AppConstant.PERMISSION_SELECT));
                    lstPermission = MainEvent.importRole(lstPermission, AppConstant.PERMISSION_PUT, (String) data.get(AppConstant.PERMISSION_PUT));
                    lstPermission = MainEvent.importRole(lstPermission, AppConstant.PERMISSION_DELETE, (String) data.get(AppConstant.PERMISSION_DELETE));

                    DefaultTableModel model = (DefaultTableModel) MainForm.getInstance().setDialog.tblPermission
                            .getModel();
                    lstPermission.forEach((row) -> {
                        MainForm.getInstance().setDialog.lstPermission.add(row);
                        String[] arrRow = {mapUser.get(row.get(0).toString()), (String)row.get(1)};
                        model.addRow(arrRow);
                    });
                }
            }, "aerospike", "set", new Qualifier("name", FilterOperation.EQ, set));
        }
    }

    public RenameSetResponse renameSet(String namespace, String oldName, String newName) {
        return MainForm.getInstance().aeClient.renameSet(namespace, oldName, newName);
    }
}
