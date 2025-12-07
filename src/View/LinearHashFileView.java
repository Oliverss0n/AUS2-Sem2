package View;

import Presenter.LinearHashFilePresenter;

import javax.swing.*;
import java.awt.*;

public class LinearHashFileView extends JFrame {

    public JButton btnInsert = new JButton("Insert");
    public JButton btnFind = new JButton("Find");
    public JButton btnDelete = new JButton("Delete");
    public JButton btnUpdate = new JButton("Update");
    public JButton btnPrint = new JButton("Print");
    public JButton btnClose = new JButton("Close");

    public JTextArea txtOutput = new JTextArea();

    public LinearHashFileView() {

        setTitle("LinearHashFile GUI");
        setSize(900, 650);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        JPanel top = new JPanel();
        top.setLayout(new FlowLayout());
        top.add(btnInsert);
        top.add(btnFind);
        top.add(btnDelete);
        top.add(btnUpdate);
        top.add(btnPrint);
        top.add(btnClose);

        add(top, BorderLayout.NORTH);

        txtOutput.setEditable(false);
        txtOutput.setFont(new Font("monospaced", Font.PLAIN, 12));

        add(new JScrollPane(txtOutput), BorderLayout.CENTER);
    }

    public void setPresenter(LinearHashFilePresenter presenter) {
        btnInsert.addActionListener(e -> presenter.onInsert());
        btnFind.addActionListener(e -> presenter.onFind());
        btnDelete.addActionListener(e -> presenter.onDelete());
        btnUpdate.addActionListener(e -> presenter.onUpdate());
        btnPrint.addActionListener(e -> presenter.onPrint());
        btnClose.addActionListener(e -> presenter.onClose());
    }


    public void append(String msg) {
        txtOutput.append(msg + "\n");
    }
}
