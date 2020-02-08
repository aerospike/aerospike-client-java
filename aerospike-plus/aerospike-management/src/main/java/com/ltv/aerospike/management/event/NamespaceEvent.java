package com.ltv.aerospike.management.event;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Date;
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

import com.ltv.aerospike.api.proto.DropNamespaceServices.DropNamespaceResponse;
import com.ltv.aerospike.api.proto.PutServices.PutResponse;
import com.ltv.aerospike.api.proto.QueryServices.QueryRequest.FilterOperation;
import com.ltv.aerospike.api.proto.QueryServices.QueryResponse;
import com.ltv.aerospike.api.proto.RenameNamespaceServices.RenameNamespaceResponse;
import com.ltv.aerospike.client.Qualifier;
import com.ltv.aerospike.management.config.AppConstant;
import com.ltv.aerospike.management.form.MainForm;
import com.ltv.aerospike.management.form.NamespaceDialog.Item;

import io.grpc.stub.StreamObserver;

public class NamespaceEvent {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(NamespaceEvent.class.getSimpleName());

    public void addRoleButtonClick(ActionEvent e) {
        Item userItem = (Item)MainForm.getInstance().namespaceDialog.cboUser.getSelectedItem();
        String user = userItem.getDescription();
        String role = MainForm.getInstance().namespaceDialog.cboRole.getSelectedItem().toString();
        DefaultTableModel model = (DefaultTableModel) MainForm.getInstance().namespaceDialog.tblRole.getModel();
        Vector<Vector> data = model.getDataVector();
        for(Vector row : data) {
            if(row.get(0).equals(user) && row.get(1).equals(role)) return;
        };

        String[] row = new String[2];
        row[0] = user;
        row[1] = role;
        model.addRow(row);
        List lstRow = new ArrayList();
        lstRow.add(userItem.getId());
        lstRow.add(role);
        MainForm.getInstance().namespaceDialog.lstRole.add(lstRow);
    }

    public Integer getNamespaceSequence() {
        return MainForm.getInstance().aeClient.getSequence(AppConstant.AEROSPIKE_NAMESPACE, AppConstant.TABLE_NAMESPACE_ID).getValue();
    }

    public void saveNamespaceButtonClick(ActionEvent e) {
        String namespace = MainForm.getInstance().namespaceDialog.txtNamespace.getText();
        HashMap<String, Object> bin = new HashMap();

        StringBuilder owner = new StringBuilder();
        StringBuilder ddl = new StringBuilder();
        StringBuilder dml = new StringBuilder();
        StringBuilder dql = new StringBuilder();
        MainForm.getInstance().namespaceDialog.lstRole.forEach((row) -> {
            switch ((String)row.get(1)) {
                case AppConstant.ROLE_DDL:
                    ddl.append(row.get(0));
                    ddl.append(",");
                    break;
                case AppConstant.ROLE_DML:
                    dml.append(row.get(0));
                    dml.append(",");
                    break;
                case AppConstant.ROLE_DQL:
                    dql.append(row.get(0));
                    dql.append(",");
                    break;
                case AppConstant.ROLE_OWNER:
                    owner.append(row.get(0));
                    owner.append(",");
                    break;
            }
        });

        bin.put(AppConstant.ROLE_OWNER, "");
        bin.put(AppConstant.ROLE_DDL, "");
        bin.put(AppConstant.ROLE_DML, "");
        bin.put(AppConstant.ROLE_DQL, "");
        bin.put(AppConstant.ROLE_DCL, "");

        if (owner.length() > 0) {
            owner.setLength(owner.length() - 1);
            bin.put(AppConstant.ROLE_OWNER, owner.toString());
        }
        if (ddl.length() > 0) {
            ddl.setLength(ddl.length() - 1);
            bin.put(AppConstant.ROLE_DDL, ddl.toString());
        }
        if (dml.length() > 0) {
            dml.setLength(dml.length() - 1);
            bin.put(AppConstant.ROLE_DML, dml.toString());
        }
        if (dql.length() > 0) {
            dql.setLength(dql.length() - 1);
            bin.put(AppConstant.ROLE_DQL, dql.toString());
        }

        int namespaceId = -1;
        String oldNamespace = MainForm.getInstance().namespaceDialog.txtOldNamespace.getText();
        if(!oldNamespace.trim().isEmpty()) {
            if(MainForm.getInstance().hashmapNamespace.get(oldNamespace) == null) return;
            namespaceId = Math.round(Float.parseFloat(MainForm.getInstance().hashmapNamespace.get(oldNamespace).get(AppConstant.KEY).toString()));
        } else {
            MainForm.getInstance().aeClient.createNamespace(namespace);
            MainForm.getInstance().loadNamespace();
            namespaceId = Math.round(Float.parseFloat(MainForm.getInstance().hashmapNamespace.get(namespace).get(AppConstant.KEY).toString()));
        }

        bin.put(AppConstant.KEY, "" + namespaceId);
        bin.put("name", namespace);
        bin.put("create_at", new Date().getTime());
        bin.put("update_at", new Date().getTime());
        bin.put("del_flag", 0);
        MainForm.getInstance().aeClient.put(new StreamObserver<PutResponse>() {
            @Override
            public void onNext(PutResponse putResponse) {

            }

            @Override
            public void onError(Throwable throwable) {
                MainForm.getInstance().lblStatus.setText("Namespace saved failed");
            }

            @Override
            public void onCompleted() {
                SwingUtilities.invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            MainForm.getInstance().namespaceDialog.setVisible(false);
                            TreePath path = MainForm.getInstance().treeNamespace.getSelectionPath();
                            if (!MainForm.getInstance().namespaceDialog.txtOldNamespace.getText().isEmpty()) {
                                DefaultTreeModel model =
                                        (DefaultTreeModel) MainForm.getInstance().treeNamespace.getModel();
                                DefaultMutableTreeNode node =
                                        (DefaultMutableTreeNode) path.getLastPathComponent();
                                node.setUserObject(
                                        MainForm.getInstance().namespaceDialog.txtNamespace.getText());
                                model.nodeChanged(node);
                                if (!namespace.equals(oldNamespace)) {
                                    MainForm.getInstance().hashmapNamespace.remove(oldNamespace);
                                    MainForm.getInstance().loadNamespace();
                                }
                            } else {
                                DefaultMutableTreeNode root =
                                        (DefaultMutableTreeNode) path
                                                .getPathComponent(0);
                                root.add(new DefaultMutableTreeNode(namespace));
                                ((DefaultTreeModel) MainForm.getInstance().treeNamespace.getModel())
                                        .nodeStructureChanged(root);
                                MainForm.getInstance().loadNamespace();
                            }
                            MainForm.getInstance().lblStatus.setText("Namespace saved successfully");
                        } catch (Exception ex) {
                            log.error("Create namespace failed", ex);
                        }
                    }
                });
            }
        }, null, AppConstant.AEROSPIKE_NAMESPACE, AppConstant.TABLE_NAMESPACE,
        "" + namespaceId, bin);
    }

    public void deleteNamespace() {
        MainForm.getInstance().buildConfirmDialog(MainForm.getInstance().confirmDialog, AppConstant.CONFIRM_DELETE,
              new JButton(AppConstant.OK) {{
                  addActionListener(new ActionListener() {
                      @Override
                      public void actionPerformed(ActionEvent actionEvent) {
                          MainForm.getInstance().aeClient.dropNamespace(
                                  new StreamObserver<DropNamespaceResponse>() {
                                      @Override
                                      public void onNext(DropNamespaceResponse dropNamespaceResponse) {
                                      }

                                      @Override
                                      public void onError(Throwable throwable) {
                                          MainForm.getInstance().lblStatus.setText("Namespace deleted failed");
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
                                                  MainForm.getInstance().lblStatus.setText("Namespace deleted successfully");
                                                  MainForm.getInstance().loadNamespaceDelay(1000l);
                                              }
                                          });
                                      }
                                  }, MainForm.getInstance().treeNamespace.getLastSelectedPathComponent().toString());
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

    public void loadTblRole(Map<String, String> mapUser) {
        String namespace = MainForm.getInstance().namespaceDialog.txtNamespace.getText();
        if(namespace != null && !namespace.isEmpty()) {
            List<Map<String, Object>> lstNamespace = new ArrayList();
            MainForm.getInstance().aeClient.query(new StreamObserver<QueryResponse>() {
                @Override
                public void onNext(QueryResponse response) {
                    lstNamespace.add(MainForm.getInstance().aeClient.parseRecord(response));
                }

                @Override
                public void onError(Throwable throwable) {
                    MainForm.getInstance().lblStatus.setText("Cann't get namespace role");
                }

                @Override
                public void onCompleted() {
                    Map<String, Object> data = lstNamespace.get(0);
                    List<List> lstRole = MainEvent.importRole(new ArrayList(), AppConstant.ROLE_OWNER, (String) data.get(AppConstant.ROLE_OWNER));
                    lstRole = MainEvent.importRole(lstRole, AppConstant.ROLE_DDL, (String) data.get(AppConstant.ROLE_DDL));
                    lstRole = MainEvent.importRole(lstRole, AppConstant.ROLE_DML, (String) data.get(AppConstant.ROLE_DML));
                    lstRole = MainEvent.importRole(lstRole, AppConstant.ROLE_DQL, (String) data.get(AppConstant.ROLE_DQL));

                    DefaultTableModel model = (DefaultTableModel) MainForm.getInstance().namespaceDialog.tblRole.getModel();
                    lstRole.forEach((row) -> {
                        MainForm.getInstance().namespaceDialog.lstRole.add(row);
                        String[] arrRow = {mapUser.get(row.get(0).toString()), (String)row.get(1)};
                        model.addRow(arrRow);
                    });
                }
            }, "aerospike", "namespace", new Qualifier("name", FilterOperation.EQ, namespace));
        }
    }

    public RenameNamespaceResponse renameNamespace(String oldName, String newName) {
        return MainForm.getInstance().aeClient.renameNamespace(oldName, newName);
    }
}
