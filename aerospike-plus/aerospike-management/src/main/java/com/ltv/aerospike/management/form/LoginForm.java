package com.ltv.aerospike.management.form;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.GridLayout;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.text.JTextComponent;
import javax.swing.tree.DefaultMutableTreeNode;

import com.apple.eawt.Application;
import com.ltv.aerospike.client.AerospikeClient;
import com.ltv.aerospike.management.component.DefaultFont;
import com.ltv.aerospike.management.component.EditableTree;
import com.ltv.aerospike.management.config.AppConstant;
import com.ltv.aerospike.management.event.LoginEvent;
import com.ltv.aerospike.management.run.Aerospike;

public class LoginForm extends JFrame {
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(LoginForm.class.getSimpleName());
    public EditableTree treeConnection = null;
    public JTextField txtConnection = new JTextField();
    public String connectionName;
    public JTextField txtHost = new JTextField();
    public JTextField txtPort = new JTextField();
    public JTextField txtUser = new JTextField();
    public JPasswordField txtPassword = new JPasswordField();
    public JButton btnLogin = new JButton("Login");
    public JLabel lblResponse = new JLabel();
    public Map<String, AerospikeClient> connection = new HashMap();
    public static LoginForm instance;
    public LoginEvent loginEvent = new LoginEvent();

    public LoginForm() {
        ImageIcon appImage = new ImageIcon(getClass().getClassLoader().getResource("logo.png"));
        if(appImage != null) {
            Application.getApplication().setDockIconImage(appImage.getImage());
            setIconImage(appImage.getImage());
        }

        // set layout
        setLayout(new BorderLayout(5,5));
        JSplitPane splitPane = new JSplitPane();
        splitPane.setResizeWeight(0.3);
        splitPane.setDividerLocation(0.3);
        add(splitPane, BorderLayout.CENTER);

        // fill layout
        splitPane.setLeftComponent(buildConnectionTree());
        splitPane.setRightComponent(buildLoginPanel());

        btnLogin.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                loginEvent.buttonLoginListener();
            }
        });

        buildFormInformation();
    }

    public static synchronized LoginForm getInstance() {
        if(instance == null) instance = new LoginForm();
        return instance;
    }

    private JPanel addLoginField(JPanel formPanel, String label, JTextComponent component) {
        JLabel lbl = new JLabel(label, SwingConstants.RIGHT);
        DefaultFont.setFont(lbl);
        DefaultFont.setFont(component);
        formPanel.add(lbl);
        formPanel.add(component);
        formPanel.add(new JLabel());

        return  formPanel;
    }

    private JPanel buildLoginPanel() {
        JPanel loginPanel = new JPanel();
        loginPanel.setAlignmentX(JComponent.CENTER_ALIGNMENT);
        loginPanel.setLayout(new GridLayout(4, 1));

        // build logo panel
        JPanel logoPanel = new JPanel();
        logoPanel.setLayout(new GridLayout(1, 3));
        JLabel lblLogo = new JLabel();
        lblLogo.setIcon(new ImageIcon(getClass().getClassLoader().getResource("login_logo.png")));
        logoPanel.add(new JLabel());
        logoPanel.add(lblLogo);
        lblLogo.setHorizontalAlignment(JLabel.CENTER);
        logoPanel.add(new JLabel());

        // build login panel
        JPanel formPanel = new JPanel();
        formPanel.setLayout(new GridLayout(7,3));
        addLoginField(formPanel, AppConstant.NAME, txtConnection);
        addLoginField(formPanel, AppConstant.HOST, txtHost);
        addLoginField(formPanel, AppConstant.PORT, txtPort);
        addLoginField(formPanel, AppConstant.USER_NAME, txtUser);
        addLoginField(formPanel, AppConstant.PASSWORD, txtPassword);

        formPanel.add(new JLabel());
        formPanel.add(btnLogin);
        btnLogin.addPropertyChangeListener(new PropertyChangeListener() {
            @Override
            public void propertyChange(PropertyChangeEvent propertyChangeEvent) {
                String property = propertyChangeEvent.getPropertyName();
                if ("Frame.active".equals(property)) {
                    if(btnLogin.getForeground().equals(Color.LIGHT_GRAY.brighter())) {
                        btnLogin.setForeground(Color.DARK_GRAY.darker());
                    } else {
                        btnLogin.setForeground(Color.LIGHT_GRAY.brighter());
                    }
                }
            }
        });
        btnLogin.setForeground(Color.LIGHT_GRAY.brighter());
        btnLogin.setSelected(true);
        formPanel.add(new JLabel());

        formPanel.add(new JLabel());
        formPanel.add(lblResponse);
        lblResponse.setText(AppConstant.LOGIN_INCORRECT);
        DefaultFont.setFont(lblResponse);
        lblResponse.setVisible(false);

        loginPanel.add(new JPanel());
        Color color = logoPanel.getBackground();
        logoPanel.setBackground(darker(color));
        formPanel.setBackground(darker(color));
        loginPanel.add(logoPanel);
        loginPanel.add(formPanel);
        loginPanel.add(new JPanel());
        return loginPanel;
    }

    private Color darker(Color color) {
        return new Color(color.getRed() - 10, color.getGreen()-10,color.getBlue()-10);
    }

    private void buildFormInformation() {
        setTitle(AppConstant.APP_NAME);
        setSize(1024, 768);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocation((Toolkit.getDefaultToolkit().getScreenSize().width) / 2 - getWidth() / 2, (Toolkit.getDefaultToolkit().getScreenSize().height) / 2 - getHeight() / 2);
        setVisible(true);
    }

    private JScrollPane buildConnectionTree() {
        DefaultMutableTreeNode top = new DefaultMutableTreeNode("CONNECTIONS");
        int count = 1;
        while (!"".equals(Aerospike.config.getConfig("connection" + count))) {
            String nodeName = Aerospike.config.getConfig("connection" + count);
            DefaultMutableTreeNode connectionNode = new DefaultMutableTreeNode(nodeName);
            top.add(connectionNode);
            connection.put(nodeName, new AerospikeClient(
                    Aerospike.config.getConfig("connection" + count + ".host"),
                    Integer.parseInt(Aerospike.config.getConfig("connection" + count + ".port")),
                    Aerospike.config.getConfig("connection" + count + ".user"),
                    Aerospike.config.getConfig("connection" + count + ".password")
            ));
            count++;
        }

        treeConnection = new EditableTree(top)  {
            @Override
            public void buildPopupMenu() {
                JPopupMenu connectionTreePopup = new JPopupMenu();
                connectionTreePopup.add(buildMenu(AppConstant.DELETE, AppConstant.DELETE));

                addMouseListener(new MouseAdapter() {
                    public void mouseReleased(MouseEvent e) {
                        if (treeConnection.getSelectionPath() != null
                            && SwingUtilities.isRightMouseButton(e)) {
                            connectionTreePopup.show((JComponent) e.getSource(), e.getX(), e.getY());
                        }
                    }
                });
            }

            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                loginEvent.deleteConnectionNode(actionEvent);
            }
        };
        JScrollPane scrollPane = new JScrollPane(treeConnection);
        scrollPane.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
        treeConnection.addTreeSelectionListener(new TreeSelectionListener() {
            public void valueChanged(TreeSelectionEvent evt) {
                loginEvent.connectionTreeSelectionListener(evt);
            }
        });
        return scrollPane;
    }
}
