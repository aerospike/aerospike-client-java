package com.ltv.aerospike.management.event;

import java.awt.event.ActionEvent;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;

import javax.swing.event.TreeSelectionEvent;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

import com.ltv.aerospike.api.proto.LoginServices.LoginResponse;
import com.ltv.aerospike.api.util.ErrorCode;
import com.ltv.aerospike.client.AerospikeClient;
import com.ltv.aerospike.management.config.AppConstant;
import com.ltv.aerospike.management.form.LoginForm;
import com.ltv.aerospike.management.form.MainForm;
import com.ltv.aerospike.management.run.Aerospike;

public class LoginEvent {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(LoginEvent.class.getSimpleName());

    public void buttonLoginListener() {
        try {
            LoginResponse response = null;
            AerospikeClient aeClient = Aerospike.aeConnector.get(
                    LoginForm.getInstance().txtHost.getText() +
                    LoginForm.getInstance().txtPort.getText());
            if(aeClient == null) {
                aeClient = new AerospikeClient(
                        LoginForm.getInstance().txtHost.getText(),
                        Integer.parseInt(LoginForm.getInstance().txtPort.getText()),
                        LoginForm.getInstance().txtUser.getText(),
                        String.valueOf(LoginForm.getInstance().txtPassword.getPassword()));
                LoginForm.getInstance().connectionName = LoginForm.getInstance().txtConnection.getText();
                Aerospike.aeConnector.put(LoginForm.getInstance().connectionName, aeClient);
                response = aeClient.connect();
            } else {
                response = aeClient.login();
            }

            if (response != null && response.getErrorCode() == ErrorCode.SUCCESS.getValue()) {
                MainForm.getInstance().lblUser.setText("Hi " + LoginForm.getInstance().txtUser.getText() + "!");
                MainForm.getInstance().setVisible(true);
                LoginForm.getInstance().setVisible(false);

                if(LoginForm.getInstance().connection.get(LoginForm.getInstance().connectionName) == null) {
                    // add node to tree
                    DefaultMutableTreeNode root = (DefaultMutableTreeNode) LoginForm.getInstance().treeConnection.getModel().getRoot();
                    root.add(new DefaultMutableTreeNode(LoginForm.getInstance().connectionName));
                    ((DefaultTreeModel) LoginForm.getInstance().treeConnection.getModel()).nodeStructureChanged(root);
                }

                LoginForm.getInstance().connection.put(LoginForm.getInstance().connectionName, aeClient);
                writeConnectionFile();
            } else {
                LoginForm.getInstance().lblResponse.setVisible(true);
            }
        } catch (Exception ex) {
            log.info("login failed!", ex);
        }
    }

    public void writeConnectionFile() throws IOException {
        StringBuilder str = new StringBuilder();
        int count = 1;
        for(Map.Entry<String, AerospikeClient> entry : LoginForm.getInstance().connection.entrySet()) {
            str.append("connection").append(count).append("=").append(entry.getKey()).append("\n");
            str.append("connection").append(count).append(".host=").append(entry.getValue().host).append("\n");
            str.append("connection").append(count).append(".port=").append(entry.getValue().port).append("\n");
            str.append("connection").append(count).append(".user=").append(entry.getValue().userName).append("\n");
            str.append("connection").append(count).append(".password=").append(entry.getValue().password).append("\n\n");
            count++;
        }
        FileOutputStream fos = new FileOutputStream("etc/server.conf");
        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
        BufferedWriter outputFile = new BufferedWriter(osw);
        try {
            outputFile.write(str.toString());
        } finally {
            outputFile.close();
            osw.close();
            fos.close();
        }
    }

    public void connectionTreeSelectionListener(TreeSelectionEvent evt) {
        if(evt.getNewLeadSelectionPath() == null) return;
        String connectionName = evt.getNewLeadSelectionPath().getLastPathComponent().toString();
        AerospikeClient ae = LoginForm.getInstance().connection.get(connectionName);
        if (ae != null) {
            LoginForm.getInstance().txtConnection.setText(connectionName);
            LoginForm.getInstance().txtHost.setText(ae.host);
            LoginForm.getInstance().txtPort.setText("" + ae.port);
            LoginForm.getInstance().txtUser.setText(ae.userName);
            LoginForm.getInstance().txtPassword.setText(ae.password);
        }
    }

    public void deleteConnectionNode(ActionEvent actionEvent) {
        try {
            DefaultMutableTreeNode selectedNode, parentNode;
            TreePath path = LoginForm.getInstance().treeConnection.getSelectionPath();
            if(path == null) return;
            selectedNode = (DefaultMutableTreeNode) path.getLastPathComponent();
            if (actionEvent.getActionCommand().equals(AppConstant.DELETE)) {
                LoginForm.getInstance().connection.remove(selectedNode.toString());
                writeConnectionFile();
                parentNode = (DefaultMutableTreeNode) selectedNode.getParent();
                int nodeIndex = parentNode.getIndex(selectedNode);
                selectedNode.removeAllChildren();
                parentNode.remove(nodeIndex);
                ((DefaultTreeModel) LoginForm.getInstance().treeConnection.getModel()).nodeStructureChanged((TreeNode) selectedNode);
            }
        } catch (Exception ex) {
            log.error("Delete connection failed", ex);
        }
    }
}
