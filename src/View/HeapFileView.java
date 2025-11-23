package View;

import Presenter.HeapFilePresenter;

import javax.swing.*;
import java.awt.*;

public class HeapFileView extends JFrame {

    public JButton btnInsert = new JButton("Insert");
    public JButton btnBatch = new JButton("Generate 100");
    public JButton btnGet = new JButton("Get by ID");
    public JButton btnDelete = new JButton("Delete by ID");
    public JButton btnPrint = new JButton("Print");
    public JButton btnClose = new JButton("Close");

    public JTextArea txtOutput = new JTextArea();

    public HeapFileView() {

        setTitle("HeapFile GUI");
        setSize(800, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        JPanel top = new JPanel();
        top.setLayout(new FlowLayout());
        top.add(btnInsert);
        top.add(btnBatch);
        top.add(btnGet);
        top.add(btnDelete);
        top.add(btnPrint);
        top.add(btnClose);

        add(top, BorderLayout.NORTH);

        txtOutput.setEditable(false);
        txtOutput.setFont(new Font("monospaced", Font.PLAIN, 12));

        add(new JScrollPane(txtOutput), BorderLayout.CENTER);
    }

    public void setPresenter(HeapFilePresenter presenter) {
        btnInsert.addActionListener(e -> presenter.onInsertRandom());
        btnBatch.addActionListener(e -> presenter.onGenerateBatch());
        btnGet.addActionListener(e -> presenter.onGet());
        btnDelete.addActionListener(e -> presenter.onDelete());
        btnPrint.addActionListener(e -> presenter.onPrint());
        btnClose.addActionListener(e -> presenter.onClose());
    }

    public void append(String msg) {
        txtOutput.append(msg + "\n");
    }
}
