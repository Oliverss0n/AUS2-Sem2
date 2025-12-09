package View;

import Presenter.MainPresenter;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.util.List;

public class MainView extends JFrame implements IMainView {

    private final JTextArea outputArea;
    private final JButton btnGenerate, btnClose;
    private final JButton btnRandomPrint;
    private final JButton btnPrintPeople;
    private final JButton btnPrintTests;

    private final JButton btnOpenDb;
    private final JButton btnCreateDb;
    private final JButton btnCloseDb;
    private final JLabel lblDbStatus;

    private final JButton[] taskButtons;
    private MainPresenter presenter;

    public MainView() {

        setTitle("WHO PCR Evidenčný systém");
        setSize(1200, 800);
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setLocationRelativeTo(null);
        setLayout(new BorderLayout(10, 10));

        JPanel dbPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        dbPanel.setBorder(BorderFactory.createTitledBorder("Database Management"));

        btnOpenDb = new JButton("Open Database");
        btnCreateDb = new JButton("Create New Database");
        btnCloseDb = new JButton("Close Database");
        lblDbStatus = new JLabel("No database open");
        lblDbStatus.setFont(new Font("Arial", Font.BOLD, 12));
        lblDbStatus.setForeground(Color.RED);

        dbPanel.add(btnOpenDb);
        dbPanel.add(btnCreateDb);
        dbPanel.add(btnCloseDb);
        dbPanel.add(Box.createHorizontalStrut(20));
        dbPanel.add(lblDbStatus);

        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
        top.setBorder(BorderFactory.createTitledBorder("Operations"));

        btnGenerate = new JButton("Generovať dáta");
        btnClose = new JButton("Close App");
        btnRandomPrint = new JButton("Náhodný výpis");
        btnPrintPeople = new JButton("Vypísať osoby");
        btnPrintTests = new JButton("Vypísať testy");

        top.add(btnGenerate);
        top.add(btnClose);
        top.add(btnRandomPrint);
        top.add(btnPrintPeople);
        top.add(btnPrintTests);

        JPanel northPanel = new JPanel(new BorderLayout());
        northPanel.add(dbPanel, BorderLayout.NORTH);
        northPanel.add(top, BorderLayout.SOUTH);

        add(northPanel, BorderLayout.NORTH);

        outputArea = new JTextArea();
        outputArea.setFont(new Font("Consolas", Font.PLAIN, 13));
        outputArea.setEditable(false);
        add(new JScrollPane(outputArea), BorderLayout.CENTER);

        JPanel left = new JPanel(new GridLayout(0, 1, 5, 5));

        String[] ops = {
                "1. Vložiť test",
                "2. Vyhľadať osobu",
                "3. Vyhľadať test",
                "4. Vložiť osobu",
                "5. Vymazať test",
                "6. Vymazať osobu",
                "7. Editovať osobu",
                "8. Editovať test"
        };

        taskButtons = new JButton[ops.length];
        for (int i = 0; i < ops.length; i++) {
            taskButtons[i] = new JButton(ops[i]);
            left.add(taskButtons[i]);
        }

        JScrollPane leftScroll = new JScrollPane(left);
        leftScroll.setPreferredSize(new Dimension(400, 0));
        add(leftScroll, BorderLayout.WEST);
    }

    @Override
    public void setPresenter(MainPresenter presenter) {
        this.presenter = presenter;

        btnOpenDb.addActionListener(e -> presenter.onOpenDatabase());
        btnCreateDb.addActionListener(e -> presenter.onCreateDatabase());
        btnCloseDb.addActionListener(e -> presenter.onCloseDatabase());

        btnGenerate.addActionListener(e -> presenter.onGenerateData());
        btnClose.addActionListener(e -> presenter.opClose());
        btnPrintPeople.addActionListener(e -> presenter.onPrintPeople());
        btnPrintTests.addActionListener(e -> presenter.onPrintTests());

        for (int i = 0; i < taskButtons.length; i++) {
            int id = i + 1;
            taskButtons[i].addActionListener(e -> presenter.onTaskSelected(id));
        }
    }


    public JButton getBtnOpenDb() {
        return btnOpenDb;
    }

    public JButton getBtnCreateDb() {
        return btnCreateDb;
    }

    public JButton getBtnCloseDb() {
        return btnCloseDb;
    }

    public void setDbStatus(String status, boolean isOpen) {
        lblDbStatus.setText(status);
        lblDbStatus.setForeground(isOpen ? Color.GREEN.darker() : Color.RED);
    }


    @Override
    public void showTable(String title, String[] columns, List<Object[]> rows) {

        DefaultTableModel model = new DefaultTableModel(columns, 0);

        for (Object[] row : rows) {
            model.addRow(row);
        }

        JTable table = new JTable(model);
        table.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);

        JScrollPane scroll = new JScrollPane(table);
        scroll.setPreferredSize(new Dimension(800, 500));

        JOptionPane.showMessageDialog(
                this,
                scroll,
                title,
                JOptionPane.INFORMATION_MESSAGE
        );
    }

    @Override
    public void showOutput(String text) {
        outputArea.setText(text != null ? text : "");
    }

    @Override
    public void appendOutput(String text) {
        outputArea.append((text != null ? text : "") + "\n");
    }

    @Override
    public String promptInput(String msg) {
        return JOptionPane.showInputDialog(this, msg);
    }

    @Override
    public void showMessage(String msg) {
        JOptionPane.showMessageDialog(this, msg);
    }
}