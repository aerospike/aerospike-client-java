package com.ltv.aerospike.management.event;

import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.swing.JComponent;
import javax.swing.SwingUtilities;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.table.DefaultTableModel;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

import com.google.gson.Gson;
import com.ltv.aerospike.api.proto.CreateIndexServices.CreateIndexRequest.IndexType;
import com.ltv.aerospike.api.proto.DeleteServices.DeleteResponse;
import com.ltv.aerospike.api.proto.GetServices.GetResponse;
import com.ltv.aerospike.api.proto.PutServices.PutResponse;
import com.ltv.aerospike.api.proto.QueryServices.QueryRequest.FilterOperation;
import com.ltv.aerospike.api.proto.QueryServices.QueryResponse;
import com.ltv.aerospike.api.proto.ShowBinServices.ShowBinResponse;
import com.ltv.aerospike.api.proto.ShowIndexServices.ShowIndexResponse;
import com.ltv.aerospike.api.proto.ShowSetServices.ShowSetResponse;
import com.ltv.aerospike.client.Qualifier;
import com.ltv.aerospike.management.config.AppConstant;
import com.ltv.aerospike.management.config.NamespaceTreeCommand;
import com.ltv.aerospike.management.config.NamespaceTreeNode;
import com.ltv.aerospike.management.form.MainForm;

import io.grpc.stub.StreamObserver;

public class MainEvent {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(MainEvent.class.getSimpleName());

    public void namespaceTreeMouseListener(MouseEvent e) {
        try {
            if (MainForm.getInstance().treeNamespace.getSelectionPath() != null && SwingUtilities
                    .isRightMouseButton(e)) {
                List<Map<String, Object>> lstUser = MainForm.getInstance().aeClient.query("aerospike", "user");
                switch (MainForm.getInstance().treeNamespace.getSelectionPath().getPathCount()) {
                    case NamespaceTreeNode.ROOT_VALUE:
                        if(lstUser.isEmpty()) MainForm.getInstance().createNamespaceMenu.setVisible(false);
                        else MainForm.getInstance().createNamespaceMenu.setVisible(true);
                        MainForm.getInstance().deleteNamespaceMenu.setVisible(false);
                        MainForm.getInstance().editNamespaceMenu.setVisible(false);
                        MainForm.getInstance().createSetMenu.setVisible(false);
                        MainForm.getInstance().deleteSetMenu.setVisible(false);
                        MainForm.getInstance().editSetMenu.setVisible(false);
                        break;
                    case NamespaceTreeNode.NAMESPACE_VALUE:
                        if(lstUser.isEmpty()) MainForm.getInstance().createNamespaceMenu.setVisible(false);
                        else MainForm.getInstance().createNamespaceMenu.setVisible(true);
                        if(lstUser.isEmpty()) MainForm.getInstance().deleteNamespaceMenu.setVisible(false);
                        else MainForm.getInstance().deleteNamespaceMenu.setVisible(true);
                        if(lstUser.isEmpty()) MainForm.getInstance().editNamespaceMenu.setVisible(false);
                        else MainForm.getInstance().editNamespaceMenu.setVisible(true);
                        MainForm.getInstance().createSetMenu.setVisible(true);
                        MainForm.getInstance().deleteSetMenu.setVisible(false);
                        MainForm.getInstance().editSetMenu.setVisible(false);
                        break;
                    case NamespaceTreeNode.SET_VALUE:
                        if(lstUser.isEmpty()) MainForm.getInstance().createNamespaceMenu.setVisible(false);
                        else MainForm.getInstance().createNamespaceMenu.setVisible(true);
                        MainForm.getInstance().deleteNamespaceMenu.setVisible(false);
                        MainForm.getInstance().editNamespaceMenu.setVisible(false);
                        MainForm.getInstance().createSetMenu.setVisible(true);
                        MainForm.getInstance().deleteSetMenu.setVisible(true);
                        MainForm.getInstance().editSetMenu.setVisible(true);
                        break;
                }
                MainForm.getInstance().connectionTreePopup.show((JComponent) e.getSource(), e.getX(), e.getY());
            }
        } catch (Exception ex) {
            log.error("Load popup menu failed");
        }
    }

    public void namespaceTreePopupListener(ActionEvent actionEvent) {
        TreePath path = MainForm.getInstance().treeNamespace.getSelectionPath();
        if(path != null && path.getPathCount() <= 3) {
            switch (actionEvent.getActionCommand()) {
                case NamespaceTreeCommand.CREATE_NAMESPACE_VALUE:
                    MainForm.getInstance().namespaceDialog.txtNamespace.setText("");
                    MainForm.getInstance().namespaceDialog.txtOldNamespace.setText("");
                    MainForm.getInstance().showNamespaceDialog();
                    break;
                case NamespaceTreeCommand.EDIT_NAMESPACE_VALUE:
                    MainForm.getInstance().namespaceDialog.txtNamespace.setText(MainForm.getInstance().treeNamespace.getLastSelectedPathComponent().toString());
                    MainForm.getInstance().namespaceDialog.txtOldNamespace.setText(MainForm.getInstance().treeNamespace.getLastSelectedPathComponent().toString());
                    MainForm.getInstance().showNamespaceDialog();
                    break;
                case NamespaceTreeCommand.DELETE_NAMESPACE_VALUE:
                    MainForm.getInstance().namespaceDialog.namespaceEvent.deleteNamespace();
                    break;
                case NamespaceTreeCommand.CREATE_SET_VALUE:
                    MainForm.getInstance().setDialog.txtSet.setText("");
                    MainForm.getInstance().setDialog.txtOldSet.setText("");
                    MainForm.getInstance().showSetDialog();
                    break;
                case NamespaceTreeCommand.EDIT_SET_VALUE:
                    MainForm.getInstance().setDialog.txtSet.setText(MainForm.getInstance().treeNamespace.getLastSelectedPathComponent().toString());
                    MainForm.getInstance().setDialog.txtOldSet.setText(MainForm.getInstance().treeNamespace.getLastSelectedPathComponent().toString());
                    MainForm.getInstance().showSetDialog();
                    break;
                case NamespaceTreeCommand.DELETE_SET_VALUE:
                    MainForm.getInstance().setDialog.setEvent.deleteSet();
                    break;
            }
        }
    }

    public void saveQueryTableRow(String key, String binName, String value, String oldValue) {
        // get request parameter
        String namespace = MainForm.getInstance().treeNamespace.getSelectionPath().getPath()[1].toString();
        String set = MainForm.getInstance().treeNamespace.getSelectionPath().getPath()[2].toString();
        Map meta = new Gson().fromJson((String)MainForm.getInstance().hashmapSet.get(namespace + "|" + set).get("meta"), Map.class);
        String type = (String) meta.get(binName);
        HashMap bin = new HashMap();
        if(AppConstant.KEY.equals(binName)) {
            GetResponse response = MainForm.getInstance().aeClient.get(namespace, set, oldValue, null);
            Map<String, Object> oldBin = MainForm.getInstance().aeClient.parseRecord(response);
            if(oldBin != null && !oldBin.isEmpty()) {
                bin.putAll(oldBin);

                MainForm.getInstance().aeClient.delete(new StreamObserver<DeleteResponse>() {
                    @Override
                    public void onNext(DeleteResponse deleteResponse) {

                    }

                    @Override
                    public void onError(Throwable throwable) {
                        log.error("Delete record failed", throwable);
                    }

                    @Override
                    public void onCompleted() {

                    }
                }, namespace, set, oldValue);
                key = value;
            }
            bin.put(binName, value);
        } else {
            switch (type) {
                case AppConstant.INTEGER:
                    bin.put(binName, Integer.parseInt(value));
                    break;
                case AppConstant.LONG:
                    bin.put(binName, Long.parseLong(value));
                    break;
                case AppConstant.FLOAT:
                    bin.put(binName, Float.parseFloat(value));
                    break;
                case AppConstant.DOUBLE:
                    bin.put(binName, Double.parseDouble(value));
                    break;
                case AppConstant.BOOLEAN:
                    bin.put(binName, Boolean.parseBoolean(value));
                    break;
                case AppConstant.DATE:
                    bin.put(binName, stringToMiliseconds(value));
                    break;
                default:
                    bin.put(binName, value);
            }
        }

        MainForm.getInstance().aeClient.put(new StreamObserver<PutResponse>() {
            @Override
            public void onNext(PutResponse putResponse) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        }, null, namespace, set, key, bin);
    }

    public void saveStructureTableRow(String column, String oldValue, String value) {
        // get request parameter
        String namespace = MainForm.getInstance().treeNamespace.getSelectionPath().getPath()[1].toString();
        String set = MainForm.getInstance().treeNamespace.getSelectionPath().getPath()[2].toString();

        Map meta = new Gson().fromJson((String)MainForm.getInstance().hashmapSet.get(namespace + "|" + set).get("meta"), Map.class);
        String binName = MainForm.getInstance().structureTable.getValueAt(
                MainForm.getInstance().structureTable.getSelectedRow(),
                MainForm.getInstance().structureTable.getColumn("bin").getModelIndex()).toString();
        String binType = MainForm.getInstance().structureTable.getValueAt(
                MainForm.getInstance().structureTable.getSelectedRow(),
                MainForm.getInstance().structureTable.getColumn("type").getModelIndex()).toString();
        if("bin".equals(column)) {
            MainForm.getInstance().aeClient.renameBin(namespace, set, oldValue, value);
            String type = AppConstant.STRING;
            if(meta.get(oldValue) != null) {
                type = (String)meta.get(oldValue);
                meta.remove(oldValue);
            }
            meta.put(value, type);

            HashMap<String, String> setInfo = new HashMap();
            setInfo.put("meta", new Gson().toJson(meta));
            MainForm.getInstance().aeClient.createSet(namespace, set, setInfo);
        } else if("type".equals(column)) {
            meta.put(binName, value);
            HashMap<String, String> setInfo = new HashMap();
            setInfo.put("meta", new Gson().toJson(meta));
            MainForm.getInstance().aeClient.createSet(namespace, set, setInfo);
        } else if("index".equals(column)) {
            IndexType indexType = IndexType.STRING;
            if (!AppConstant.STRING.equals(binType)) indexType = IndexType.NUMERIC;
            if(value != null && !value.trim().isEmpty() && oldValue != null && !oldValue.trim().isEmpty() && !oldValue.equals(value)) {
                MainForm.getInstance().aeClient.dropIndex(namespace, oldValue);
                MainForm.getInstance().aeClient.createIndex(namespace, set, binName, value, indexType);
            } else if(value != null && !value.trim().isEmpty()) {
                MainForm.getInstance().aeClient.createIndex(namespace, set, binName, value, indexType);
            } else if(oldValue != null && !oldValue.trim().isEmpty()) {
                MainForm.getInstance().aeClient.dropIndex(namespace, oldValue);
            }
        }
    }

    public void queryTableOnDelete() {
        String namespace = MainForm.getInstance().treeNamespace.getSelectionPath().getPath()[1].toString();
        String set = MainForm.getInstance().treeNamespace.getSelectionPath().getPath()[2].toString();
        DefaultTableModel model = (DefaultTableModel) MainForm.getInstance().queryTable.getModel();
        int[] rows = MainForm.getInstance().queryTable.getSelectedRows();
        for(int i = 0; i < rows.length; i++) {
            MainForm.getInstance().aeClient.delete(
                    namespace,
                    set,
                    MainForm.getInstance().queryTable.getValueAt(
                            rows[i],
                            MainForm.getInstance().queryTable.getColumn(AppConstant.KEY).getModelIndex()).toString());
        }
        MainForm.getInstance().queryTable.removeSelectedRows();
    }

    public void structureTableOnDelete() {
        String namespace = MainForm.getInstance().treeNamespace.getSelectionPath().getPath()[1].toString();
        String set = MainForm.getInstance().treeNamespace.getSelectionPath().getPath()[2].toString();
        DefaultTableModel model = (DefaultTableModel) MainForm.getInstance().structureTable.getModel();
        int[] rows = MainForm.getInstance().structureTable.getSelectedRows();
        for(int i = 0; i < rows.length; i++) {
            MainForm.getInstance().aeClient.dropBin(namespace, set, MainForm.getInstance().structureTable.getValueAt(rows[i], 0).toString());
        }
        MainForm.getInstance().structureTable.removeSelectedRows();
    }

    public void namespaceTreeSelectionChanged(TreeSelectionEvent evt) {
        if(evt != null && evt.getNewLeadSelectionPath() != null) {
            DefaultTableModel queryModel = (DefaultTableModel) MainForm.getInstance().queryTable.getModel();
            DefaultTableModel structureModel = (DefaultTableModel) MainForm.getInstance().structureTable.getModel();
            queryModel.setRowCount(0);
            structureModel.setRowCount(0);
            if (evt.getNewLeadSelectionPath().getPathCount() == 3) {
                setOnClick();
            } else if (evt.getNewLeadSelectionPath().getPathCount() == 2) {
                namespaceOnClick();
            }
        }
    }

    public void namespaceOnClick() {
        // SHOW NAMESPACE ROLE TABLE
        List<Map<String, Object>> lstUser = new ArrayList();
        MainForm.getInstance().aeClient.query(new StreamObserver<QueryResponse>() {
            @Override
            public void onNext(QueryResponse queryResponse) {
                Map user = MainForm.getInstance().aeClient.parseRecord(queryResponse);
                if(user != null && user.get(AppConstant.KEY) != null) lstUser.add(user);
            }

            @Override
            public void onError(Throwable throwable) {
                MainForm.getInstance().lblStatus.setText("Namespace data loaded failed");
            }

            @Override
            public void onCompleted() {
                Map<String, String> mapUser = lstUser.stream().collect(Collectors.toMap(
                        x -> x.get(AppConstant.KEY).toString(),
                        x -> x.get("name").toString()
                ));

                // load tbl role
                String namespace = MainForm.getInstance().treeNamespace.getLastSelectedPathComponent().toString();
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
                            if(lstUser.isEmpty()) return;
                            SwingUtilities.invokeLater(new Runnable() {
                                @Override
                                public void run() {
                                    Map<String, Object> data = lstNamespace.get(0);
                                    List<List> lstRole = importRole(new ArrayList(), AppConstant.ROLE_OWNER, (String) data .get(AppConstant.ROLE_OWNER));
                                    lstRole = importRole(lstRole, AppConstant.ROLE_DDL, (String) data.get(AppConstant.ROLE_DDL));
                                    lstRole = importRole(lstRole, AppConstant.ROLE_DML, (String) data.get(AppConstant.ROLE_DML));
                                    lstRole = importRole(lstRole, AppConstant.ROLE_DQL, (String) data.get(AppConstant.ROLE_DQL));

                                    String[] structureHeader = {"user","role"};
                                    DefaultTableModel model = new DefaultTableModel(structureHeader,0);
                                    MainForm.getInstance().structureTable.setModel(model);
                                    lstRole.forEach(row -> {
                                        String[] arrRow = {mapUser.get(row.get(0).toString()), (String) row.get(1)};
                                        model.addRow(arrRow);
                                    });
                                }
                            });
                        }
                    }, "aerospike", "namespace", new Qualifier("name", FilterOperation.EQ, namespace));
                }
            }
        }, "aerospike", "user");

        // SHOW SET OF NAMESPACE TABLE
        String namespace = MainForm.getInstance().treeNamespace.getLastSelectedPathComponent().toString();
        MainForm.getInstance().aeClient.showSet(new StreamObserver<ShowSetResponse>() {
            @Override
            public void onNext(ShowSetResponse response) {
                SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        Map<String, String> mapBins = response.getSetsMap();
                        if(!mapBins.values().iterator().hasNext()) return;
                        Map firstRow = new Gson().fromJson(mapBins.values().iterator().next(), Map.class);
                        Set<String> header = firstRow.keySet();
                        DefaultTableModel model = new DefaultTableModel(header.toArray(new String[header.size()]), 0);
                        MainForm.getInstance().queryTable.setModel(model);
                        mapBins.forEach((k,v) -> {
                            Map<String, Object> mapBin = new Gson().fromJson(v, Map.class);
                            Map<String, Object> bins = mapBin.entrySet().stream().collect(Collectors.toMap(
                               x -> x.getKey(),
                               x -> {
                                   if(x.getKey().equals(AppConstant.KEY)) return "" + Math.round(Double.parseDouble(x.getValue().toString()));
                                   else return x.getValue();
                               }
                            ));
                            model.addRow(header.stream().map(x -> bins.get(x)).collect(Collectors.toList()).toArray());
                        });
                    }
                });
            }

            @Override
            public void onError(Throwable throwable) {
                MainForm.getInstance().lblStatus.setText("Show records failed");
            }

            @Override
            public void onCompleted() {
            }
        }, namespace);
    }

    public static List<List> importRole(List<List> roles, String role, String data) {
        if(data != null && !data.isEmpty()) {
            String[] datas = data.split(",");
            for(String row : datas) {
                List lstRow = new ArrayList();
                lstRow.add(Long.parseLong(row));
                lstRow.add(role);
                roles.add(lstRow);
            }
        }
        return roles;
    }

    public void setOnClick() {
        if(MainForm.getInstance().queryFlag) {
            MainForm.getInstance().queryFlag = false;
            return;
        }
        String set = MainForm.getInstance().treeNamespace.getLastSelectedPathComponent().toString();
        String namespace = MainForm.getInstance().treeNamespace.getSelectionPath().getPath()[1].toString();

        MainForm.getInstance().aeClient.showBin(new StreamObserver<ShowBinResponse>() {
            @Override
            public void onNext(ShowBinResponse response) {
                MainForm.getInstance().aeClient.showIndex(new StreamObserver<ShowIndexResponse>() {
                    @Override
                    public void onNext(ShowIndexResponse indexResponse) {
                        SwingUtilities.invokeLater(new Runnable() {
                            public void run() {
                                // get index column data on structure table
                                Map<String, String> mapIndexs = indexResponse.getIndexsMap();
                                Map indexStore = mapIndexs.entrySet().stream().collect(Collectors.toMap(
                                        x -> {
                                            Map mapBin = new Gson().fromJson(x.getValue(), Map.class);
                                            return mapBin.get("namespace") + "|" + mapBin.get("set") + "|" + mapBin.get("bin");
                                            },
                                        x -> (new Gson().fromJson(x.getValue(), Map.class)).get("name")
                                ));
                                String prefix = namespace + "|" + set + "|";

                                Map<String, String> mapBins = response.getBinsMap();
                                // get header column
                                if(MainForm.getInstance().hashmapSet.get(namespace + "|" + set) == null) return;
                                if(MainForm.getInstance().hashmapSet.get(namespace + "|" + set).get("meta") == null) return;
                                Map<String, Object> meta = new Gson().fromJson(MainForm.getInstance().hashmapSet.get(namespace + "|" + set).get("meta").toString(), Map.class);

                                // build structure table
                                String[] structureHeader = {"bin","type","index"};
                                DefaultTableModel structureModel = new DefaultTableModel(structureHeader,0);
                                MainForm.getInstance().structureTable.setModel(structureModel);
                                meta.forEach((k,v) -> {
                                    String[] indexRow = {k.toString(),
                                                         (String)v,
                                                         (String)indexStore.get(prefix + k)};
                                    structureModel.addRow(indexRow);
                                });

                                // build query table
                                DefaultTableModel queryModel = new DefaultTableModel(meta.keySet().toArray(new String[meta.size()]), 0);
                                MainForm.getInstance().queryTable.setModel(queryModel);
                                mapBins.forEach((k,v) -> {
                                    Map<String, Object> mapBin = new Gson().fromJson(v, Map.class);
                                    queryModel.addRow(meta.entrySet().stream().map(x -> {
                                        if(x.getValue().equals(AppConstant.DATE) && mapBin.get(x.getKey()) != null) {
                                            return milisecondsToString(Long.parseLong((String)mapBin.get(x.getKey())));
                                        } else {
                                            return mapBin.get(x.getKey());
                                        }
                                    }).collect(Collectors.toList()).toArray());
                                });
                            }
                        });
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        MainForm.getInstance().lblStatus.setText("Show indexs failed");
                    }

                    @Override
                    public void onCompleted() {
                    }
                }, namespace);

            }

            @Override
            public void onError(Throwable throwable) {
                MainForm.getInstance().lblStatus.setText("Show records failed");
            }

            @Override
            public void onCompleted() {
            }
        }, namespace, set);
    }

    public String milisecondsToString(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        return sdf.format(new Date(time));
    }

    public long stringToMiliseconds(String date) {
        long time = 0;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            time = sdf.parse(date).getTime();
        } catch (Exception ex) {
            MainForm.getInstance().lblStatus.setText("Cannot convert " + date + " to date time.");
        }
        return time;
    }

    public void runQuery() {
        String query = MainForm.getInstance().areaQuery.getSelectedText();
        if(query == null || query.trim().isEmpty()) {
            query = MainForm.getInstance().areaQuery.getText();
            if(query == null || query.trim().isEmpty()) {
                return;
            }
        }
        if(query.contains(";")) {
            String[] queries = query.split(";");
            if(!queries[queries.length - 1].trim().isEmpty()) query = queries[queries.length - 1];
            else if(queries.length > 1) query = queries[queries.length - 2];
        }
        Pattern p = Pattern.compile("'(.*?)'");
        Matcher m = p.matcher(query);
        List<String> paramList = new ArrayList();
        // get all string param to List
        while(m.find()) {
            paramList.add(m.group(1));
        }
        // replace all string param to %s
        query = m.replaceAll("%s");
        query = standardizeText(query);
        String[] words = query.split(" ");
        if(words.length < 4) {
            MainForm.getInstance().lblStatus.setText("Query syntax is incorrect");
            return;
        }
        if(!words[0].toUpperCase().equals("SELECT")) {
            MainForm.getInstance().lblStatus.setText("'SELECT' is required");
            return;
        };
        if(!words[1].toUpperCase().equals("*")) {
            MainForm.getInstance().lblStatus.setText("'*' is required");
            return;
        }
        if(!words[2].toUpperCase().equals("FROM")) {
            MainForm.getInstance().lblStatus.setText("'FROM' is required");
            return;
        }
        if(!words[3].contains(".")) {
            MainForm.getInstance().lblStatus.setText("Query object syntax is <namespace>.<set> ");
            return;
        }
        String[] setName = words[3].split("\\.");
        String namespace = setName[0];
        String set = setName[1];
        if(namespace.trim().isEmpty()) {
            MainForm.getInstance().lblStatus.setText("Namespace is required");
            return;
        }

        if(namespace.trim().isEmpty()) {
            MainForm.getInstance().lblStatus.setText("Set is required");
            return;
        }

        if(words.length > 4 && words.length < 8) {
            MainForm.getInstance().lblStatus.setText("Query syntax is incorrect");
            return;
        }

        TreePath path = findTreePath(namespace, set);
        if(path != null) {
            if(words.length >= 8) MainForm.getInstance().queryFlag = true;
            MainForm.getInstance().treeNamespace.setSelectionPath(path);
        }

        if(words.length < 8) return;

        // get header column
        Map<String, Object> meta = new Gson().fromJson(MainForm.getInstance().hashmapSet.get(namespace + "|" + set).get("meta").toString(), Map.class);
        DefaultTableModel queryModel = new DefaultTableModel(meta.keySet().toArray(new String[meta.size()]), 0);
        MainForm.getInstance().queryTable.setModel(queryModel);

        if(!words[4].toUpperCase().equals("WHERE")) {
            MainForm.getInstance().lblStatus.setText("'WHERE' is required");
            return;
        }
        
        int paramIndex = 0;
        List<Qualifier> qualifiers = new ArrayList();
        boolean checkAnd = false;
        for(int i = 5; i < words.length; i++) {
            if(checkAnd) {
                if(words[i].toUpperCase().equals("AND")) {
                    i++;
                } else {
                    MainForm.getInstance().lblStatus.setText("'AND' operator is required");
                    return;
                }
            }
            int paramNumber = 2;
            String type = (String)meta.get(words[i]);

            if(i+1 < words.length && words[i+1].toUpperCase().equals("BETWEEN")) {
                paramNumber = 4;
                // if param type is string fill data from List to Array
                if(type.equals(AppConstant.STRING)) {
                    words[i+2] = paramList.get(paramIndex);
                    words[i+4] = paramList.get(paramIndex + 1);
                    paramIndex = paramIndex + 2;
                }
            } else {
                // if param type is string fill data from List to Array
                if(type.equals(AppConstant.STRING)) {
                    words[i+2] = paramList.get(paramIndex);
                    paramIndex++;
                }
            }

            if(i + paramNumber >= words.length) {
                MainForm.getInstance().lblStatus.setText("'WHERE' condition is incorrect");
                return;
            }

            switch (words[i+1].toUpperCase()) {
                case "=":
                    qualifiers.add(buildQualifier(words[i], type, FilterOperation.EQ, words[i+2]));
                    break;
                case "BETWEEN":
                    qualifiers.add(buildQualifier(words[i], type, FilterOperation.BETWEEN, words[i+2], words[i+4]));
                    break;
                case "START":
                    qualifiers.add(buildQualifier(words[i], type, FilterOperation.START_WITH, words[i+2]));
                    break;
                case "END":
                    qualifiers.add(buildQualifier(words[i], type, FilterOperation.ENDS_WITH, words[i+2]));
                    break;
                case "LIKE":
                    qualifiers.add(buildQualifier(words[i], type, FilterOperation.CONTAINING, words[i+2]));
                    break;
                case ">":
                    qualifiers.add(buildQualifier(words[i], type, FilterOperation.GT, words[i+2]));
                    break;
                case ">=":
                    qualifiers.add(buildQualifier(words[i], type, FilterOperation.GTEQ, words[i+2]));
                    break;
                case "<":
                    qualifiers.add(buildQualifier(words[i], type, FilterOperation.LT, words[i+2]));
                    break;
                case "<=":
                    qualifiers.add(buildQualifier(words[i], type, FilterOperation.LTEQ, words[i+2]));
                    break;
                case "!=":
                    qualifiers.add(buildQualifier(words[i], type, FilterOperation.NOTEQ, words[i+2]));
                    break;
            }
            i = i + paramNumber;
            checkAnd = true;
        }

        try {
            List<Map<String, Object>> lstResult = MainForm.getInstance().aeClient.query(namespace, set, qualifiers.toArray(new Qualifier[qualifiers.size()]));
            if(lstResult != null && !lstResult.isEmpty()) {
                lstResult.forEach(mapBin -> {
                    queryModel.addRow(meta.entrySet().stream().map(x -> {
                        if(x.getValue().equals(AppConstant.DATE) && mapBin.get(x.getKey()) != null) {
                            return milisecondsToString((Long)mapBin.get(x.getKey()));
                        } else {
                            return mapBin.get(x.getKey());
                        }
                    }).collect(Collectors.toList()).toArray());
                });
            }
        } catch (InterruptedException e) {
            MainForm.getInstance().lblStatus.setText("Query data failed");
            log.error("Query data failed", e);
        }
    }

    private Qualifier buildQualifier(String field, String type, FilterOperation filterOperation, String param1) {
        return buildQualifier(field, type, filterOperation, param1, null);
    }
    
    private Qualifier buildQualifier(String field, String type, FilterOperation filterOperation, String param1, String param2) {
        switch (type) {
            case AppConstant.INTEGER:
                Integer intValue1 = Integer.parseInt(param1);
                Integer intValue2 = null;
                if(param2 != null && !param2.trim().isEmpty()) intValue2 = Integer.parseInt(param2);
                return new Qualifier(field, filterOperation, intValue1, intValue2);
            case AppConstant.LONG:
                Long longValue1 = Long.parseLong(param1);
                Long longValue2 = null;
                if(param2 != null && !param2.trim().isEmpty()) longValue2 = Long.parseLong(param2);
                return new Qualifier(field, filterOperation, longValue1, longValue2);
            case AppConstant.FLOAT:
                Float floatValue1 = Float.parseFloat(param1);
                Float floatValue2 = null;
                if(param2 != null && !param2.trim().isEmpty()) floatValue2 = Float.parseFloat(param2);
                return new Qualifier(field, filterOperation, floatValue1, floatValue2);
            case AppConstant.DOUBLE:
                Double doubleValue1 = Double.parseDouble(param1);
                Double doubleValue2 = null;
                if(param2 != null && !param2.trim().isEmpty()) doubleValue2 = Double.parseDouble(param2);
                return new Qualifier(field, filterOperation, doubleValue1, doubleValue2);
            case AppConstant.BOOLEAN:
                Boolean booleanValue1 = Boolean.parseBoolean(param1);
                Boolean booleanValue2 = null;
                if(param2 != null && !param2.trim().isEmpty()) booleanValue2 = Boolean.parseBoolean(param2);
                return new Qualifier(field, filterOperation, booleanValue1, booleanValue2);
            case AppConstant.DATE:
                Long dateValue1 = stringToMiliseconds(param1);
                Long dateValue2 = null;
                if(param2 != null && !param2.trim().isEmpty()) dateValue2 = stringToMiliseconds(param2);
                return new Qualifier(field, filterOperation, dateValue1, dateValue2);
            default:
                return new Qualifier(field, filterOperation, param1, param2);
        }
    }

    private TreePath findTreePath(String namespace, String set) {
        DefaultMutableTreeNode root = (DefaultMutableTreeNode) MainForm.getInstance().treeNamespace.getModel().getRoot();
        Enumeration<TreeNode> enumeration = root.children();
        while (enumeration.hasMoreElements()) {
            DefaultMutableTreeNode namespaceNode = (DefaultMutableTreeNode) enumeration.nextElement();
            if (namespaceNode.toString().equalsIgnoreCase(namespace)) {
                Enumeration<TreeNode> setEnumeration = namespaceNode.children();
                while (setEnumeration.hasMoreElements()) {
                    DefaultMutableTreeNode setNode = (DefaultMutableTreeNode) setEnumeration.nextElement();
                    if (setNode.toString().equalsIgnoreCase(set)) {
                        return new TreePath(setNode.getPath());
                    }
                }
            }
        }
        return null;
    }

    private String standardizeText(String query) {
        query = query.replace("\n", "");
        query = query.replace("\r", "");
        query = query.replace("\t", "");
        query = query.replace("=", " = ");
        query = query.replace(">", " > ");
        query = query.replace("<", " < ");
        query = query.replace(">  =", " >= ");
        query = query.replace("<  =", " <= ");
        query = query.replace("  ", " ");
        query = query.replace("  ", " ");
        query = query.replace("  ", " ");
        query = query.replace("  ", " ");
        query = query.replace("  ", " ");
        query = query.replace("  ", " ");
        query = query.replace("  ", " ");
        query = query.replace("  ", " ");
        query = query.replace("  ", " ");
        query = query.replace("  ", " ");
        query = query.replace(";", "");
        return query.trim();
    }
}
